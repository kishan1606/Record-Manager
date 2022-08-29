Record Manager

--------------------------------------
Running the Program:

Step 1 - Go to Project root (assign3) using Terminal.
Step 2 - $ make clean (to delete old compiled .o files)
Step 3 - $ make (to compile all project files) 
Step 4 - $ make run (to run "test_assign3_1.c" file)

To run compile test expression related files:

Step 1 - Repeat Step 1 to 3 from previous steps.
Step 2 - $ make test_expr (to compiletest expression related files "test_expr.c")
Step 3 - $ make run_expr (to run "test_expr.c" file)

--------------------------------------
Function Description:
--------------------------------------
1. functions for table and record manager management
--------------------------------------

a) initRecordManager ()
	-The record manager is started and initialized using this function. To initialize the storage manager, we used initStorageManager(). 

b) shutdownRecordManager()
	- This function shuts down the record manager. Also, free up all of the memory that has been assigned to the record manager.

c) createTable()
	- This function creates a table and initializes the Buffer Pool. For initializing, we use the LRU page replacement policy. It writes schema and other meta data on page 0 of the newly created file and sets the first page to page 1. It then opens a page file with the file page name as the table name, writes the schema to the first page, and then closes the page file.

d) openTable()
	- This function opens the table with table name "name". The page is then placed in the buffer pool. The schema is then read from the table. And the page is then unpinned and written back to disk.

e) closeTable()
	- As indicated by the'rel', this function closes the table. It saves the table's data to a page file before shutting down the buffer pool.

f) deleteTable()
	- This function deletes the table named 'name'. It deletes the page from the disk and deallocates any memory that has been assigned through the storage manager.

g) getNumTuples()
	- The total number of rows in the table indicated by the'rel' is returned by this function.

--------------------------------------
2. functions for handling the records in a table
--------------------------------------

a) insertRecord()
	- This function is used to inserts a new record in the table at specific locations. The record is inserted in free space. We pin the page, find the data pointer, and add a '+' to show that this record was recently added. We mark the page dirty after the record is inserted so that the page's data is written back to the disk. The page is then unpinned.

b) deleteRecord()
	- This function is used to delete a record in the table having Record ID 'id'. We first pin the page and then delete the record and set the first bit to '-' which indicates that this record has been deleted and is no longer needed, and then we free up the space so that it can be used by a new record later.  We mark the page dirty after the record is deleted so that the page's data is written back to the disk. The page is then unpinned.

c) updateRecord()
	- This function is used to update a table record that is referred by the parameter "record." It uses the table's information to find the page where the record is located and pins that page to the buffer pool. The record is updated after the page is pinned.  We mark the page dirty after the record is updated so that the page's data is written back to the disk. The page is then unpinned.

d) getRecord()
	- This function retrieves a record from a table with the Record ID "id." The record is retrieved after pinning the page in the buffer pool. The page is then unpinned.

--------------------------------------
3. functions related to scans
--------------------------------------

a) startScan()
	- This function is used to begin scanning tuples depending on the data structure provided as an input. It sets up the RM_ScanHandle data structure as well as the custom DS scan variables.

b) next()
	-This function returns the next tuple from the record that fulfills the startScan criteria. We verify all of the tuple values in the table, then pin the page with that tuple, browse to the data location, copy data into a temporary buffer, and evaluate the test expression. If the condition is met, we unpin the page and return RC OK. If the condition is not met, we return an error code RC_RM_NO_MORE_TUPLES.

c) closeScan() 
	-This function pauses the scan and closes the scan procedure. We first check to see if the scan was completed. If it was, we unpin the page and reset all scan process relevant variables in our table's DS. After that, we free up the space that the metadata has taken up.

--------------------------------------
4. functions for dealing with schemas
-------------------------------------- 

a) getRecordSize()
	- This function returns the size in bytes of a record in the given schema.

b) freeSchema()
	- This function is used to free up the space related with specific schema in the memory. It deallocates all the memory space occupied by schemas.

c) createSchema()
	- This function creates a new schema based on passed parameter and assigns the memory to it.

--------------------------------------
5. function for dealing with attribute values and creating records
--------------------------------------

a) createRecord()
	- This function creates a new record in the schema specified by the 'schema' constraint and assigns it to the 'record' parameter. We allocate sufficient memory space for the new record, as well as memory space for record size. We also append a '-' to the first position, indicating that this is a new blank record, and then '0' after that. After that, we assign this new record to it.

b) attrOffset()
	- This function inserts the offset in bytes from the original location to the record's particular property into the'result' parameter. We loop through the schema's attributes until the required attribute number is obtained, then we attach the size required by each attribute to the pointer recursively.

c) freeRecord()
	- This function releases the memory space reserved for the record given through the parameter and its data.

d) getAttr()
	- This function retrieves an attribute value from the provided schema's record. The parameter contains the record, schema, and attribute number whose data is to be obtained. The attribute information is returned to the value location. Integer, string, or float values can be returned.

e) setAttr()
	-This function is used to set the attribute value in the provided schema's record. The data to be saved in the variable is given by value and can be an integer, string, or float. We need execute different operations depending on the datatype of the attribute.