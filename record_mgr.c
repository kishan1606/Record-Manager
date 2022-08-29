#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15;

RecordManager *recordManager;

// This Function was taken from rm_serializer.c file
RC attrOffset (Schema *schema, int attrNum, int *result){
	int offset = 1;
	int attrPos = 0;

	for(attrPos = 0; attrPos < attrNum; attrPos++)
		switch (schema->dataTypes[attrPos]){
			case DT_STRING:
				offset += schema->typeLength[attrPos];
				break;
			case DT_INT:
				offset += sizeof(int);
				break;
			case DT_FLOAT:
				offset += sizeof(float);
				break;
			case DT_BOOL:
				offset += sizeof(bool);
				break;
		}

	*result = offset;
	return RC_OK;
}

RC record_checks(Record *record){
	if(record == NULL){
		return RC_RECORD_NULL_ERROR;
	}else{
		return RC_OK;
	}
}

RC schema_checks(Schema *schema){
	if(schema == NULL){
		return RC_SCHEMA_NULL_ERROR;
	}else{
		return RC_OK;
	}
}

RC value_checks(Value *value){
	if(value == NULL){
		return RC_VALUE_NULL_ERROR;
	}else{
		return RC_OK;
	}
}

RC scan_checks(RM_ScanHandle *scan){
	if(scan == NULL){
		return RC_VALUE_NULL_ERROR;
	}else{
		return RC_OK;
	}
}

// Its the global Check Function which call all other check functions
// Input (All Optional) -> Record, Schema, Value, Scan, Checktype
// Output -> RC_CODE
RC global_checks(Record *record, Schema *schema, Value *value, RM_ScanHandle *scan, int checkType){
	if(checkType == 0){
		return record_checks(record);
	}else if(checkType == 1){
		return schema_checks(schema);
	}else if(checkType == 2){
		return value_checks(value);
	}else if(checkType == 3){
		if(record_checks(record) == RC_OK){
			return schema_checks(schema);
		}else{
			return record_checks(record);
		}
	}else if(checkType == 4){
		return scan_checks(scan);
	}
}

// dayaTypeAssinger - Supporting Function to assign & copy datatype values
// Uses typeChoser to determine which to use 0 -> System Defined Values 1 -> User Defined Values
// FunType determiners getter/setter function 0 -> Setter 1 -> Getter
// Input -> Schema, TypeChoser, AttrNum, Value, DataPointer
// Output -> RC_CODE
RC dataTypeAssigner(Schema *schema,int typeChoser, int attrNum, Value *value, char *dp, int funType, Value **doubleValue){
	if((*schema).dataTypes[attrNum] == DT_STRING){
		// String DataType Detected
		if(typeChoser != 0){
			// Use System Defined Storage type
			// Future Work
			return RC_OK;
		}else{
			// Use System
			// Check Function type Getter/Setter
			if(funType != 0){
				// Getter
				Value *attr = (Value*) malloc(sizeof(Value));
				(*attr).dt = DT_STRING;
				(*attr).v.stringV = (char *) malloc((*schema).typeLength[attrNum] + 1);
				strncpy((*attr).v.stringV, dp, (*schema).typeLength[attrNum]);
				(*attr).v.stringV[(*schema).typeLength[attrNum]] = '\0';
				*doubleValue = attr;
				return RC_OK;
			}else{
				// Setter
				strncpy(dp, (*value).v.stringV, (*schema).typeLength[attrNum]);
				dp+= (*schema).typeLength[attrNum];
				return RC_OK;
			}
		}
	}else if((*schema).dataTypes[attrNum] == DT_INT){
		if(typeChoser == 0){
			// Use System Defined
			// Check Function type Getter/Setter
			if(funType != 0){
				// Getter
				int val = 0;
				Value *attr = (Value*) malloc(sizeof(Value));

				memcpy(&val, dp, sizeof(int));
				(*attr).v.intV = val;
				(*attr).dt = DT_INT;
				*doubleValue = attr;
				return RC_OK;
			}else{
				// Setter
				*(int *)dp = (*value).v.intV;
				dp += sizeof(int);
				return RC_OK;
			}
		}else{
			// Use Custom Defined
			// Future Work
			return RC_OK;
		}
	}else if((*schema).dataTypes[attrNum] == DT_FLOAT){
		if(typeChoser == 0){
			// Use System Defined
			// Check Function type Getter/Setter
			if(funType != 0){
				// Getter
				float val;
				Value *attr = (Value*) malloc(sizeof(Value));
				(*attr).dt = DT_FLOAT;
				memcpy(&val, dp, sizeof(float));
				(*attr).v.floatV = val;
				*doubleValue = attr;
				return RC_OK;		
			}else{
				// Setter
				*(float *)dp = (*value).v.floatV;
				dp += sizeof(float);
				return RC_OK;
			}
		}else{
			// Use Custom Defined
			// Future Work
			return RC_OK;
		}
	}else if((*schema).dataTypes[attrNum] == DT_BOOL){
		if(typeChoser == 0){
			// Use System Defined
			// Check Function type Getter/Setter
			if(funType != 0){
				// Getter
				Value *attr = (Value*) malloc(sizeof(Value));
				bool val;
				memcpy(&val,dp, sizeof(bool));
				(*attr).v.boolV = val;
				(*attr).dt = DT_BOOL;
				*doubleValue = attr;
				return RC_OK;
			}else{
				// Setter
				*(bool *)dp = (*value).v.boolV;
				dp += sizeof(bool);
				return RC_OK;
			}
		}else{
			// Use Custom Defined
			// Future Work
			return RC_OK;
		}
	}else{
		// No DataType exist
		// Return Error
		return RC_SCHEMA_DATA_TYPE_ERROR;
	}
}

// getFreeSlot -> Checks for free slots
// Input -> Data Pointer, RecordSize
// Output -> Integer
int getFreeSlot(char *data, int recordSize){
	int r=0, totlslts = PAGE_SIZE / recordSize, r_index = -1;
	
	while(r < totlslts){
		if('+' != data[recordSize * r]){
			r_index = r;
			break;
		}
		r++;
	}
	return r_index;
}

// initRecordManager -> Initialization
// Inputs -> managementData
// Output -> RC_CODE
extern RC initRecordManager (void *mgmtData){
	initStorageManager();
	return RC_OK;
}

// shutdownRecordManager -> Shuts down & frees the space
// Inputs -> None
// Output -> RC_CODE
extern RC shutdownRecordManager(){
	recordManager = NULL;
	free(recordManager);
	return RC_OK;
}

// setPageHandle -> Used to set values
// Helper Function
// Inputs -> PageHandle, Schema
// Outputs -> None
extern void setPageHandle(char *pageHandle, Schema *schema){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK){

	}else{
		// Everything is okay
		int i = 0;
		while(i < (*schema).numAttr){
			strncpy(pageHandle, (*schema).attrNames[i], ATTRIBUTE_SIZE);
	       	pageHandle += ATTRIBUTE_SIZE;
			*(int*)pageHandle = (int)(*schema).dataTypes[i];
			pageHandle += sizeof(int);
			*(int*)pageHandle = (int) (*schema).typeLength[i];
			pageHandle += sizeof(int);

			i++;
		}
	}
}

// increment_Page_Handle -> Used to Increment Page Handle
// Helper Function
// Inputs -> pageHandle, Number
// Output -> None
extern void increment_Page_Handle(int *pageHandle, int number){
	if(number == 1){
		*pageHandle += sizeof(int);
	}
	else{
		*pageHandle += number;
	} 
}

// createTable -> Used to create table based on specified schema
// Uses increment_Page_Handle as a helper function
// Input -> Name, Schema
// Output -> RC_CODE
extern RC createTable (char *name, Schema *schema){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK)
		return return_code;
	else{
		// Everything is okay
		if(name == ((char *)0)){
			RC_message = "Table name can not be null ";
        	return RC_NULL_IP_PARAM;
		}else{
			SM_FileHandle fHndl;
			char dt[PAGE_SIZE];
			char *pgHndl = dt;
			int rslt, k;

			recordManager = (RecordManager*) malloc(sizeof(RecordManager));
			initBufferPool(&recordManager->buffPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
			
			*(int*)pgHndl = 0; 
			increment_Page_Handle(&pgHndl,1);
			
			*(int*)pgHndl = 1;
			increment_Page_Handle(&pgHndl,1);

			*(int*)pgHndl = (*schema).numAttr;
			increment_Page_Handle(&pgHndl,1); 

			*(int*)pgHndl = (*schema).keySize;
			increment_Page_Handle(&pgHndl,1);
			
			setPageHandle(pgHndl, schema);

			return_code = createPageFile(name);

			if((return_code != RC_OK))
				return return_code;
			else{
				return_code = openPageFile(name, &fHndl);
				if(return_code != RC_OK)
					return return_code;
				else{
					return_code = writeBlock(0, &fHndl, dt);
					if((return_code != RC_OK))
						return return_code;
					else{
						return_code = closePageFile(&fHndl);
						if((return_code != RC_OK))
							return return_code;
						else
							return RC_OK;
					}
				}
			}
		}
		return RC_OK;
	}
}

// set_Schema -> Used to set the Desired Schema
// Helper Function
// Inputs -> Schema, attribute count
// Output -> None
extern void Set_Schema(Schema *schema, int attributeCount){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK){

	}else{
		// Everything is okay
		(*schema).numAttr = attributeCount;
		(*schema).attrNames = (char**) malloc(sizeof(char*) *attributeCount);
		(*schema).typeLength = (int*) malloc(sizeof(int) *attributeCount);
		(*schema).dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);

		int i=0;
		while(i < attributeCount){
			(*schema).attrNames[i] = (char*) malloc(ATTRIBUTE_SIZE);
			i++;
		}
	}
}

// set_Schema -> Used to set the Desired Attributes for the Schema
// Helper Function
// Inputs -> Schema, pageHandle, attributeSize
// Output -> None
extern void Set_Schema_AttributeNames(Schema *schema, char *pageHandle, int attributesize){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK){

	}else{
		// Everything is okay
		int i=0;
		while(i < (*schema).numAttr){
			// we can also do snprintf(sprintf(schema->attrNames[k],attributesize, "%s",pageHandle))
			// Both give the same result
			strncpy((*schema).attrNames[i], pageHandle, attributesize);
			increment_Page_Handle(&pageHandle,attributesize);
		
			(*schema).dataTypes[i]= *(int*) pageHandle;
			increment_Page_Handle(&pageHandle,1);

			(*schema).typeLength[i]= *(int*)pageHandle;
			increment_Page_Handle(&pageHandle,1);

			i++;
		}
	}
}

// openTable -> Used to open table
// Uses Helper function set_Schema and Schema_AttributeNames
// Input -> Rel, Name
// Output -> RC_CODE
extern RC openTable (RM_TableData *rel, char *name){
	if(rel == NULL){
		return RC_RELATION_NULL_ERROR;
	}else{
		if(name == ((char *)0)){
			RC_message = "Table name can not be null ";
        	return RC_NULL_IP_PARAM;
		}else{
			int attributeCount;
			SM_PageHandle pgHndl;
			Schema *schema;

			(*rel).mgmtData = recordManager;
			(*rel).name = name;

			if(pinPage(&recordManager->buffPool, &recordManager->pageHndl, 0) != RC_OK){
				return RC_PIN_PAGE_FAILED;
			}

			pgHndl = (char*) (*recordManager).pageHndl.data;
	
			(*recordManager).totalTupleCnt= *(int*)pgHndl;
			increment_Page_Handle(&pgHndl,1);

			(*recordManager).firstFreePage= *(int*) pgHndl;
			increment_Page_Handle(&pgHndl,1);
			
			attributeCount = *(int*)pgHndl;
			increment_Page_Handle(&pgHndl,1);

			schema = (Schema*) malloc(sizeof(Schema));
			Set_Schema(schema, attributeCount);
			Set_Schema_AttributeNames(schema, pgHndl, ATTRIBUTE_SIZE);

			(*rel).schema = schema;	

			if(unpinPage(&recordManager->buffPool, &recordManager->pageHndl) != RC_OK){
				return RC_UNPIN_PAGE_FAILED;
			}

			if(forcePage(&recordManager->buffPool, &recordManager->pageHndl)){
				return RC_FORCE_PAGE_FAILED;
			}
			return RC_OK;
		}
	}
	return RC_OK;
} 
		
// closeTable -> Used to close the table
// Input -> Rel
// Output -> RC_CODE
extern RC closeTable (RM_TableData *rel){
	if(rel == NULL){
		return RC_RELATION_NULL_ERROR;
	}else{
		RecordManager *rMgnr = (*rel).mgmtData;

		// TODO
		if(shutdownBufferPool(&rMgnr->buffPool) == RC_OK){
			return shutdownBufferPool(&rMgnr->buffPool);
		}else{
			return RC_OK;
		}
		return RC_OK;
	}
}

// deleteTable -> Used to delete the table
// Input -> Name
// Output -> RC_CODE
extern RC deleteTable (char *name){
	if(name == ((char *)0)){
        RC_message = "Table name can not be null ";
        return RC_NULL_IP_PARAM;
    }
	if(destroyPageFile(name) != RC_OK){
		return RC_FILE_DESTROY_FAILED;
	}else{
		return RC_OK;	
	}
	return RC_OK;
}

// getNumTuples -> Used to get number of tuples
// Input -> Rel
// Output -> tupleCount
extern int getNumTuples (RM_TableData *rel){
	if(rel == NULL){
		return RC_RELATION_NULL_ERROR;
	}else{
		RecordManager *rMgnr = (*rel).mgmtData;
		return (*rMgnr).totalTupleCnt;
	}
}

// insertRecord -> Used to insert a new record
// Inputs -> Rel, Record
// Output -> RC_CODE
extern RC insertRecord (RM_TableData *rel, Record *record){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	
	// Perform Global checks
	return_code = global_checks(record, NULL, NULL, NULL, 0);

	if(return_code != RC_OK){
		// Record Error
		return return_code;
	}else{
		// Everything OK
		RecordManager *rMgnr;
		RID *rID;
		int rcdSize;
		char *dt, *sp;

		rMgnr = (*rel).mgmtData;
		rID = &record->id;
		(*rID).page = (*rMgnr).firstFreePage;

		pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, (*rID).page);

		rcdSize = getRecordSize((*rel).schema);
		dt = (*rMgnr).pageHndl.data;

		(*rID).slot = getFreeSlot(dt, rcdSize);

		while((*rID).slot == -1){
			unpinPage(&rMgnr->buffPool, &rMgnr->pageHndl);	
			(*rID).page++;
			pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, (*rID).page);
			dt = (*rMgnr).pageHndl.data;
			(*rID).slot = getFreeSlot(dt, rcdSize);
		}
		sp = dt;
		
		markDirty(&rMgnr->buffPool, &rMgnr->pageHndl);

		sp += (rcdSize * (*rID).slot);
		*sp = '+';
		memcpy(1+sp, (*record).data + 1, rcdSize - 1);
		unpinPage(&rMgnr->buffPool, &rMgnr->pageHndl);
		(*rMgnr).totalTupleCnt += 1;
		pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, 0);
	}

	return RC_OK;
}

// deleteRecord -> Used to delete a record
// Inputs -> Relation, ID
// Output -> RC_CODE
extern RC deleteRecord (RM_TableData *rel, RID id){
	// Check if rel is not null
	if(rel == NULL){
		return RC_RELATION_NULL_ERROR;
	}else{
		RecordManager *rMgnr;
		int rcdSize;
		char *dt;

		rMgnr = (*rel).mgmtData;
		pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, id.page);
		
		(*rMgnr).firstFreePage = id.page;
		
		rcdSize = getRecordSize((*rel).schema);
		dt = (*rMgnr).pageHndl.data;
		dt += (rcdSize * id.slot);
		*dt = '-';

		markDirty(&rMgnr->buffPool, &rMgnr->pageHndl);
		unpinPage(&rMgnr->buffPool, &rMgnr->pageHndl);	
	}

	return RC_OK;
}

// updateRecord -> Used to update the record
// Inputs -> Rel, Record
// Output -> RC_CODE
extern RC updateRecord (RM_TableData *rel, Record *record){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	
	// Perform Global checks
	return_code = global_checks(record, NULL, NULL, NULL, 0);

	if(return_code != RC_OK){
		// Record Error
		return return_code;
	}else{
		// Everything OK
		RecordManager *rMgnr;
		int rcdSize;
		char *dt;

		rMgnr = (*rel).mgmtData;
		pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, (*record).id.page);

		rcdSize = getRecordSize((*rel).schema);
		dt = (*rMgnr).pageHndl.data;
		dt += (rcdSize * (*record).id.slot);
		*dt = '+';

		memcpy(1+dt, (*record).data+1, rcdSize-1);
		markDirty(&rMgnr->buffPool, &rMgnr->pageHndl);
		unpinPage(&rMgnr->buffPool, &rMgnr->pageHndl);
		return_code = RC_OK;
	}

	return return_code;
}

// getRecord -> Used to reterive a record
// Inputs -> Rel, Record id, Record
// Output -> RC_CODE
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	
	// Perform Global checks
	return_code = global_checks(record, NULL, NULL, NULL, 0);

	if(return_code != RC_OK){
		return return_code;
	}else{
		RecordManager *rMgnr;
		int rcdSize;
		char *dp;

		rMgnr = (*rel).mgmtData;
		pinPage(&rMgnr->buffPool, &rMgnr->pageHndl, id.page);

		rcdSize = getRecordSize((*rel).schema);
		dp = (*rMgnr).pageHndl.data;
		dp += (rcdSize * id.slot);

		if('+' != *dp){
			return_code = RC_RM_NO_TUPLE_WITH_GIVEN_RID;
		}else{
			char *dt = (*record).data;
			(*record).id = id;
			memcpy(++dt, dp+1, rcdSize - 1);
			return_code = RC_OK;
		}
		unpinPage(&rMgnr->buffPool, &rMgnr->pageHndl);
		// return_code = RC_OK;
	}

	return return_code;
}

// startScan -> Scan records for the given condition
// Inputs -> Rel, Scan, Condition
// Output -> RC_CODE
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	// Perform Global checks 
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform scan checks
	return_code = global_checks(NULL, NULL, NULL, scan, 4);
	if(return_code != RC_OK){
		return return_code;
	}else{
		if(cond != NULL){
			// Condition is supplied
			RecordManager *scnMgnr, *tblMgnr;

			openTable(rel, "ScanTable");

			scnMgnr = (RecordManager*) malloc(sizeof(RecordManager));
			tblMgnr = (*rel).mgmtData;
			(*tblMgnr).totalTupleCnt = ATTRIBUTE_SIZE;

			(*scan).mgmtData = scnMgnr;
			(*scnMgnr).rID.page = 1;
			(*scnMgnr).rID.slot = 0;
			(*scnMgnr).totalScannedCount = 0;
			(*scnMgnr).condition = cond;
			(*scan).rel = rel;
			return_code = RC_OK;
		}else{
			return_code = RC_SCAN_CONDITION_NOT_FOUND;
		}
	}
	return return_code;
}

// next -> Scans every record and if match found stores the record
// Input -> Scan, Record
// Output -> RC_CODE
extern RC next (RM_ScanHandle *scan, Record *record){
	// Perform Global checks 
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(record, NULL, NULL, NULL, 0);
	if(return_code != RC_OK){
		// RECORD ERROR
		return return_code;
	}else{
		// Check for scan
		if(scan == NULL){
			return RC_SCAN_NOT_FOUND;
		}else{
			// Everything is OK
			RecordManager *scnMgnr;
			scnMgnr = (*scan).mgmtData;
			
			if((*scnMgnr).condition != NULL){
				// Test Expression Found
				RecordManager *tblMgnr;
				tblMgnr = (*scan).rel->mgmtData;
				
				Schema *schema = (*scan).rel->schema;
				Value *rslt = (Value *) malloc(sizeof(Value));
				char *dt, *dp;
				int rcdSize, totlSlts, scnCnt, tplCnt;

				rcdSize = getRecordSize(schema);
				scnCnt = (*scnMgnr).totalScannedCount;
				tplCnt = (*tblMgnr).totalTupleCnt;
				totlSlts = PAGE_SIZE / rcdSize;

				if(tplCnt != 0){
					// Can Continue
					while(scnCnt < tplCnt - 1){
						if(0 < scnCnt){
							(*scnMgnr).rID.slot++;
							if((totlSlts) <= (*scnMgnr).rID.slot){
								(*scnMgnr).rID.page++;
								(*scnMgnr).rID.slot = 0;
							}else{
								// Do Nothing
							}
						}else{
							(*scnMgnr).rID.page = 1;
							(*scnMgnr).rID.slot = 0;
						}

						pinPage(&tblMgnr->buffPool, &scnMgnr->pageHndl, (*scnMgnr).rID.page);

						(*record).id.page = (*scnMgnr).rID.page;
						(*record).id.slot = (*scnMgnr).rID.slot;
						dt = (*scnMgnr).pageHndl.data;
						dt += ((*scnMgnr).rID.slot * rcdSize);
						dp = (*record).data;
						*dp = '-';
						
						memcpy(++dp, 1 + dt, rcdSize - 1);

						(*scnMgnr).totalScannedCount++;
						scnCnt++;

						evalExpr(record, schema, (*scnMgnr).condition, &rslt); 

						if((*rslt).v.boolV == FALSE){
							// Condition Doesn't Meet
							return_code = RC_CONDITION_NOT_MET;
						}else{
							unpinPage(&tblMgnr->buffPool, &scnMgnr->pageHndl);	
							return_code = RC_OK;
							return RC_OK;
						}
					}
					unpinPage(&tblMgnr->buffPool, &scnMgnr->pageHndl);
					(*scnMgnr).totalScannedCount = 0;
					(*scnMgnr).rID.page = 1;
					(*scnMgnr).rID.slot = 0;
					return_code = RC_RM_NO_MORE_TUPLES;

				}else{
					return_code = RC_RM_NO_MORE_TUPLES;
				}
			}else{
				// Test Expression Not Found
				return_code = RC_SCAN_CONDITION_NOT_FOUND;
			}
		}
	}
	return return_code;
}

// closeScan -> Used to close the scan
// Input -> ScanHandle
// Output -> RC_CODE
extern RC closeScan (RM_ScanHandle *scan){
	// Check if scan exists
	if(scan == NULL){
		// No scan exist 
		// Return Error
		return RC_SCAN_NOT_FOUND;
	}else{
		// Scan Exist
		RecordManager *scnMgnr;
		scnMgnr = (*scan).mgmtData;
		RecordManager *rMgnr;
		rMgnr = (*scan).rel->mgmtData;

		if((*scnMgnr).totalScannedCount < 0){
			return RC_SCAN_COM_ERROR;
		}else{
			unpinPage(&rMgnr->buffPool, &scnMgnr->pageHndl);
			(*scnMgnr).totalScannedCount = 0;
			(*scnMgnr).rID.page = 1;
			(*scnMgnr).rID.slot = 0;
		}		
    	free((*scan).mgmtData);
		(*scan).mgmtData = NULL;
		return RC_OK;
	}
}

// getRecordSize -> Used to get size of the record of the given schema
// Inputs -> Schema
// Output -> Record Size
extern int getRecordSize (Schema *schema){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK)
		return return_code;
	else{
		int size = 0, i = 0;

		while(i < (*schema).numAttr){
			if((*schema).dataTypes[i] == DT_BOOL)
				size+= sizeof(bool);
			else if((*schema).dataTypes[i] == DT_FLOAT)
				size+= sizeof(float);
			else if((*schema).dataTypes[i] == DT_INT)
				size+= sizeof(int);
			else
				size += (*schema).typeLength[i];
			i++;
		}

		return ++size;
	}
}

// createSchema -> Creates new schema based on values passed
// Input -> NumAttr, attrnames, datatypes, typeLength, keySize, Keys
// Output -> Created Schema Object
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){	
	Schema *createNewSchema = (Schema *) malloc(sizeof(Schema));
	(*createNewSchema).dataTypes = dataTypes;
	(*createNewSchema).numAttr = numAttr;
	(*createNewSchema).keyAttrs = keys;
	(*createNewSchema).attrNames = attrNames;	
	(*createNewSchema).typeLength = typeLength;
	(*createNewSchema).keySize = keySize;
	return createNewSchema; 
}

// freeSchema -> Deallocate/Remove schema from Memory
// Input -> Schema
// Output -> RC_CODE
extern RC freeSchema (Schema *schema){
	// De-allocating memory space occupied by 'schema'
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(NULL, schema, NULL, NULL, 1);
	if(return_code != RC_OK)
		return return_code;
	else{
		free(schema);
	}
	return return_code;
}

// freeRecord -> Deallocate/Remove record from Memory
// Input -> Record
// Output -> RC_CODE
extern RC freeRecord (Record *record){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = record_checks(record);
	if(return_code != RC_OK)
		return return_code;
	else{
		free(record);
	}
	return return_code;
}

// createRecord -> Create new Record for the supplied schema
// Input -> Record, Schema
// Output -> RC_CODE
extern RC createRecord (Record **record, Schema *schema){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Perform record checks
	return_code = global_checks(*record, schema, NULL, NULL, 1);
	if(return_code != RC_OK)
		return return_code;
	else{
		// Everything is okay
		Record *createNewRecord;
		int newrRrcdSize;
		char *dp;

		createNewRecord = (Record*) malloc(sizeof(Record));
		newrRrcdSize = getRecordSize(schema);
		(*createNewRecord).id.page = (*createNewRecord).id.slot = -1;
		(*createNewRecord).data= (char*) malloc(newrRrcdSize);
		dp = (*createNewRecord).data;
		*dp = '-'; // Tombstone mechanism
		*(++dp) = '\0'; // Adding NULL after Tombstone Mechanism
		*record = createNewRecord;
		return RC_OK;
	}
}

// setAttr -> Used to set Attribute according to the schema passed
// Makes use of dataTypeAssigner Function
// Input -> Record, Schema, Attribute Number, Value
// Output -> RC_CODE
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	
	int offset = 0;
	char *dp = (*record).data;
	
	attrOffset(schema, attrNum, &offset);
	dp += offset;
	
	return_code = dataTypeAssigner(schema, 0, attrNum, value, dp, 0, NULL);		
	return return_code;
}

// getAttr -> Used to get Value from the given record from the specified schema.
// Makes use of dataTypeAssigner Function
// Input -> Record, Schema, Attribute Number, Value
// Output -> RC_CODE
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	int offset = 0;
	char *dp = (*record).data;
	
	attrOffset(schema, attrNum, &offset);
	dp += offset;
	
	if(attrNum != 1){
		(*schema).dataTypes[attrNum] = (*schema).dataTypes[attrNum];
	}else{
		(*schema).dataTypes[attrNum] = 1;
	}
	
	return_code = dataTypeAssigner(schema, 0, attrNum, NULL, dp, 1, value);
	return return_code;
}
