#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

//SIZES
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)


//
typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

//Row structure
typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
}Row;

// sql statments
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType type;
  Row row_to_insert; // only use when inserting
} Statement;

//command definition
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;


typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT,PREPARE_SYNTAX_ERROR } PrepareResult;


const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

//Row functions
void serialize_row (Row * src, void* dest){
    memcpy(dest + ID_OFFSET,&(src->id),ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(src->username),USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email),EMAIL_SIZE);
}

void deserialize_row (void* src, Row* dest){
    memcpy(&(dest->id), &src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), &src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), &src + EMAIL_OFFSET, EMAIL_SIZE);
}

//Table
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  uint32_t num_rows;
  void* pages[TABLE_MAX_PAGES];
} Table;

//table functions
void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if(page == NULL){
        //allocate memory when accessing null page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    if (table->num_rows > TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert,row_slot(table,table->num_rows++));
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table){
    Row row;
    for(uint32_t i = 0; i < table->num_rows;i++){
        deserialize_row(table->pages[i],&row);
        printf(&row);
    }
    return EXECUTE_SUCCESS;
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}


void printPrompt (){printf("db > ");}

void read_input (InputBuffer* input_buffer){
    ssize_t byte_read = 
        getline(&(input_buffer->buffer),&(input_buffer->buffer_length),stdin);
    if(byte_read <= 0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    
    //Ignore trailing new line
    input_buffer->buffer_length = byte_read -1;
    input_buffer->buffer[byte_read-1] = 0;
}
//delete input_buffer
void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

Table* newTable(){
    Table* table = (Table*)malloc(sizeof(Table));
    for(u_int32_t i = 0; i < TABLE_MAX_PAGES; i++){
        table->pages[i] = NULL;
    }
    return table;
}

void freeTable(Table* table){
    for(u_int32_t i = 0; i < TABLE_MAX_PAGES; i++){
        free(table->pages[i]);
    }
    free (table);
}

//meta commands implementation
MetaCommandResult do_meta_command(InputBuffer* input_buffer,Table* table){
    if(strcmp(input_buffer->buffer,".exit") == 0){
        close_input_buffer(input_buffer);
        freeTable(table);
        exit(EXIT_SUCCESS);
    }
    else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

//Prepare statement
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        int args_assigned  = sscanf(
            input_buffer->buffer,"insert %d %s %s",&(statement->row_to_insert.id),
            statement->row_to_insert.username,
            statement->row_to_insert.email
        );
        if(args_assigned != 3){
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}   

//execute stament
ExecuteResult execute_statement(Statement* statement,Table* table){
    switch (statement->type) {
        case (STATEMENT_INSERT):
            printf("exe_insert");
            return execute_insert(statement,table);

        case (STATEMENT_SELECT):
            return execute_select(table);
        default:
            printf("DLLM\n");
            return EXECUTE_SUCCESS;
    }  
}


int main (int argc, char* args[]){
    InputBuffer* input_buffer = new_input_buffer();
    Table* table = newTable();
    while(true){
        printPrompt();
        read_input(input_buffer);

       if(input_buffer->buffer[0] == '.'){
            switch(do_meta_command(input_buffer,table)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer,&statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
            
            
        }
        switch(execute_statement(&statement,table)){
            case (EXECUTE_SUCCESS):
                printf("Executed\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error Table Full.\n");
                break;
        }
        
       
    }
}