import pandas as pd
import snowflake.connector as sf
import streamlit as st
import os

User1 = {'User_name': 'Password',
         'User_name1': 'Password1'}

#selected_user = st.selectbox('Select your user', list(User1.keys()))


def database_schema_list():
    conn_sf = sf.connect(
        user='user_name',
        password='password',
        account='account_name',
        warehouse=''
    )
    cur = conn_sf.cursor()
    return conn_sf, cur


def IT(user):
    # Check if the provided user exists in the dictionary
    if user in User1:
        password = User1[user]
        conn_sf = sf.connect(
            user=user,
            password=password,
            account='',
            warehouse=''
        )
        cur1 = conn_sf.cursor()
        return
    else:
        print(f"User '{user}' not found in the credentials dictionary.")
        return None  # You can handle this case according to your needs


def Test(user):
    if user in User1:
        password = User1[user]
        conn_sf = sf.connect(
            user=user,
            password=password,
            account='',
            warehouse=''
        )
        cur2 = conn_sf.cursor()
        return conn_sf, cur2
    else:
        print(f"User '{user}' not found in the credentials dictionary.")
        return None  # You can handle this case according to your needs



def home_page():
    connection, cur = database_schema_list()

    FETCHING_DATABASE_NAME = cur.execute(
        "SELECT DATABASE_NAME, SCHEMA_NAME FROM DB.SCHEMA.TABLE_LIST").fetchall()
    df = pd.DataFrame(FETCHING_DATABASE_NAME, columns=['DATABASE_NAME', 'SCHEMA_NAME'])
    database_list = df['DATABASE_NAME'].dropna().tolist()
    Userenvironment = ['Test', 'It', 'LOCAL']
    User_Login_Environment = st.sidebar.selectbox('Select your object', Userenvironment)
    User_Object_selection = ['Table', 'View']
    Object_layer = ["Semantic", "Core", "Stage"]
    database_list = df['DATABASE_NAME'].dropna().tolist()
    schema_list = df['SCHEMA_NAME'].tolist()
    Select_Database_Name = st.selectbox("Select a Database Name:", database_list)

    SchemaList = []
    Schemaname1 = ['SCHEMA1','SCHEMA2]
    Schemaname2 = ['SCHEMA3','SCHEMA4']
    

    ##Schema handling

    if Select_Database_Name == 'DB1':
        SchemaList.append(Schemaname1)
    elif Select_Database_Name == 'DB2':
        SchemaList.append(Schemaname2)
    else:
        SchemaList.append(schemaname3)

    flatList = [element for innerList in SchemaList for element in innerList]
    Select_Schema_Name = st.selectbox("Select a Schema:", flatList)
    Enter_Table_name = st.text_input("Enter Table Name:")
    Column_Mapping = st.text_area("Enter Column Names (comma-separated):").split(',')
    Object_selection = st.selectbox('Select your object', User_Object_selection)
    Object_layer1 = st.radio("Object Lyer", Object_layer)
    path = f'/Users/la20270159/Documents/STREALIT/{Select_Database_Name}.{Select_Schema_Name}.{Enter_Table_name}'

    User1 = {'USER': 'PASSWORD'}

    selected_user = st.selectbox('Select your user', list(User1.keys()))
    if Object_selection == User_Object_selection[1]:
        view_name = st.text_input("Enter View name if required:")
    else:
        pass

    #### View function

    def create_view(view_name, Column_Mapping, Select_Database_Name, Select_Schema_Name, table_name, Userenvironment,
                    User_Login_Environment):
        create_view_command = f"CREATE OR REPLACE VIEW {Select_Database_Name}.{Select_Schema_Name}.{view_name} (\n"
        Create_view_command1 = ''
        # Add each column to the command with a line break
        ## Core view
        if Object_layer1 == Object_layer[1]:
            for column in Column_Mapping:
                create_view_command += f"  {column.split()[0].upper()},\n"
                Create_view_command1 += f" a.{column.split()[0]} as {column.split()[0]},\n"
            # Add the additional column and close the command
            create_view_command3 = f"   Create_Dt,Create_Ts,Change_Ts,Etl_Create_Batch_Sk,Etl_Change_Batch_Sk As Etl_Batch_Sk,2147483647  As Etl_Delete_Batch_Sk,Etl_Change_Ts As Eff_Start_Ts,'9999-12-31 00:00:00' As Eff_End_Ts,Etl_Action_Cd,Etl_Create_Ts,Etl_Change_Ts) \n"
            Command5 = "a.Create_Dt  AS  Create_Dt\n,a.Created  AS  Create_Ts  \n,a.Last_Modified  AS  Change_Ts\n,a.SEQUENCE_NUM  AS  SEQUENCE_NUM\n,a.BATCH_SK  AS  BATCH_SK  \n,a.INSERT_TS  AS  ETL_CREATE_TS  \n,a.OP_CODE  AS  OP_CODE\n,a.PROCESSED_FLAG  AS  PROCESSED_FLAG  \n,CURRENT_TIMESTAMP(0)  AS  EFF_START_TS\n,'9999-12-31 00:00:00'  AS  EFF_END_TS"
            command4 = "as select"
            create_view_command += create_view_command3 + command4 + Create_view_command1 + create_view_command3 + Command5
            create_view_command += f"from {Select_Database_Name}.{Select_Schema_Name}_app.{Enter_Table_name}"
        # Execute the command
        else:
            for column in Column_Mapping:
                create_view_command += f"  {column.split()[0].upper()},\n"
                Create_view_command1 += f" a.{column.split()[0]} as {column.split()[0]},\n"
            # Add the additional column and close the command
            create_view_command3 = f"  CREATE_DT,\nCREATE_TS,\nCHANGE_TS,\nSEQUENCE_NUM,\nBATCH_SK,\nETL_CREATE_TS,\nOP_CODE,\nPROCESSED_FLAG,\nEFF_START_TS,\nEFF_END_TS"
            command4 = "as select"
            create_view_command += create_view_command3 + command4 + Create_view_command1 + create_view_command3
            create_view_command += f"from {Select_Database_Name}.{Select_Schema_Name}_app.{Enter_Table_name} as a"


        try:

            if User_Login_Environment == Userenvironment[0] or User_Login_Environment == Userenvironment[1]:
                connection, cursor = IT(selected_user)
                connection1, cursor1 = Test(selected_user)
                if User_Login_Environment == Userenvironment[0]:

                    print(1)
                    print(cursor1)
                    cursor1.execute(f"use role {Select_Database_Name}_{Select_Schema_Name}_main_role")
                    print(2)

                    cursor1.execute(f"{create_view_command}")

                else:
                    cursor.execute(f"use role {Select_Database_Name}_{Select_Schema_Name}_main_role")
                    print(1)
                    cursor.execute(f"{create_view_command}")
            else:
                with open(f'{path}_VIEW', 'w') as file:
                    # Write content to the file
                    file.write(f'{create_view_command}')
        except Exception as e:
            st.error(f"Error occurred: {str(e)}")

        ## stage view

    #### Table Function

    def create_table(Enter_Table_name, Column_Mapping, Select_Database_Name, Select_Schema_Name, Userenvironment,
                     User_Login_Environment):
        create_table_command = f"CREATE TABLE {Select_Database_Name}.{Select_Schema_Name}.{Enter_Table_name} (\n"
        comment2 = 'COMMENT'
        ## core table
        if Object_layer1 == Object_layer[1]:
            # Add each column to the command with a line break
            for column in Column_Mapping:
                create_table_command += f"    {column} {comment2} '{column.split()[0].upper()}',\n"
            # Add the additional column and close the command
            create_table_command += f"    CREATE_DT DATE {comment2} 'Created Date'\n,CREATE_TS TIMESTAMP_NTZ(0) {comment2} 'Create Datetime',\nCHANGE_TS TIMESTAMP_NTZ(0) {comment2} 'Change Datetime',\nETL_CREATE_BATCH_SK NUMBER(38,0) NOT NULL {comment2} 'ETL Create Batch Key',\nETL_CHANGE_BATCH_SK NUMBER(38,0) NOT NULL {comment2} 'ETL Change Batch Key',\nETL_ACTION_CD VARCHAR(1) NOT NULL {comment2} 'ETL Action Code',\nETL_CREATE_TS TIMESTAMP_NTZ(0) {comment2} 'EDW Record Creation Timestamp',\nETL_CHANGE_TS TIMESTAMP_NTZ(0) {comment2} 'EDW Record Changed Timestamp'\n);"
        ## semantic
        elif Object_layer1 == Object_layer[0]:
            for column in Column_Mapping:
                create_table_command += f"    {column} {comment2} '{column.split()[0].upper()}',\n"
            # Add the additional column and close the command
            create_table_command += f" CREATE_DT DATE COMMENT 'CREATE DATE',\n ETL_BATCH_SK NUMBER(38,0) NOT NULL COMMENT 'EXTRACT TRANSFORM LOAD BATCH KEY',\nETL_CHANGE_TS TIMESTAMP_NTZ(0) COMMENT 'EXTRACT TRANSFORM LOAD CHANGE DATETIME'\n);"
        ##Stage
        else:
            for column in Column_Mapping:
                create_table_command += f"    {column} ,\n"
            # Add the additional column and close the command
            create_table_command += f"CREATE_DT DATE,\nCREATED VARCHAR(19),\nLAST_MODIFIED VARCHAR(19),\nSEQUENCE_NUM VARCHAR(19),\nBATCH_SK VARCHAR(4000),\nINSERT_TS TIMESTAMP_NTZ(0),\nOP_CODE VARCHAR(1),\nPROCESSED_FLAG VARCHAR(1)\n);"

        # check()
        try:

            if User_Login_Environment == Userenvironment[0] or User_Login_Environment == Userenvironment[1]:
                connection, cursor = IT(selected_user)
                connection1, cursor1 = Test(selected_user)
                if User_Login_Environment == Userenvironment[0]:

                    print(1)
                    print(cursor1)
                    cursor1.execute(f"use role {Select_Database_Name}_{Select_Schema_Name}_main_role")
                    print(2)

                    cursor1.execute(f"{create_table_command}")

                else:
                    cursor.execute(f"use role {Select_Database_Name}_{Select_Schema_Name}_main_role")
                    print(1)
                    cursor.execute(f"{create_table_command}")
            else:
                with open(f'{path}.ddl', 'w') as file:
                    # Write content to the file
                    file.write(f'{create_table_command}')
        except Exception as e:
            st.error(f"Error occurred: {str(e)}")

    #### Object call

    if Object_selection == User_Object_selection[1]:
        if st.button("Create View", key="create_view_button"):
            if view_name and Column_Mapping:
                create_view(view_name, Column_Mapping, Select_Database_Name, Select_Schema_Name, Enter_Table_name,
                            Userenvironment, User_Login_Environment)
                st.success(f"View '{view_name}' created successfully!")
            else:
                st.error("Please enter a view name, select a prefix, and provide at least one column.")


    else:
        if st.button("Create Table", key="create_table_button"):
            if Enter_Table_name and Column_Mapping:
                create_table(Enter_Table_name, Column_Mapping, Select_Database_Name, Select_Schema_Name,
                             Userenvironment, User_Login_Environment)
                st.success(f"Table '{Enter_Table_name}' created successfully!")
            else:
                st.error("Please enter a table name, select a prefix, and provide at least one column.")




def another_page():
    conn_sf = sf.connect(
        user='USER',
        password='PASS',
        account='',
        warehouse=''
    )


    cur = conn_sf.cursor()

    FETCHING_DATABASE_NAME = cur.execute(
        "SELECT DATABASE_NAME, SCHEMA_NAME FROM GBI_FINANCE_CORE_ETL_DB.ITUNES_EDR_CORE.TABLE_LIST").fetchall()
    df = pd.DataFrame(FETCHING_DATABASE_NAME, columns=['DATABASE_NAME', 'SCHEMA_NAME'])
    database_list = df['DATABASE_NAME'].dropna().tolist()
    schema_list = df['SCHEMA_NAME'].tolist()
    print(database_list)
    print(schema_list)
    Userenvironment = ['Test', 'It', 'LOCAL']
    User_Login_Environment = st.selectbox('Select your object', Userenvironment)
    DatabaseName = database_list
    SchemaName = schema_list
    Select_Database_Name = st.selectbox("Select a Database Name:", DatabaseName)

    SchemaList = []
    Schemaname1 = ['SCHEMA1','SCHEMA2']
    Schemaname2 = ['SCHEMA3']
    Schemaname3 = ['schema4']
    
    ##Schema handling

    if Select_Database_Name == 'db1':
        SchemaList.append(Schemaname1)
    elif Select_Database_Name == '':
        SchemaList.append(Schemaname2)
    else:
        SchemaList.append(schemaname3)
    flatList = [element for innerList in SchemaList for element in innerList]
    Select_Schema_Name = st.selectbox("Select a Schema:", flatList)
    Enter_Table_name = st.text_input("Enter Table Name:")
    Column_Mapping = tuple(st.text_area("Enter Column Names (comma-separated):").split(','))
    Alter_select = ['Add', 'Drop', 'Rename', 'Set_data_retention']
    Select_alter_Process =  st.selectbox("Alter Select", Alter_select)

    path = f'/Users/la20270159/Documents/STREALIT/{Select_Database_Name}.{Select_Schema_Name}.{Enter_Table_name}'

    User1 = {'user':'pass'}

    selected_user = st.selectbox('Select your user', list(User1.keys()))


    def check():
        if os.path.exists(f"{path}"):
            os.remove(f"{path}")
        else:
            print("The file does not exist")
        return path

    if Select_alter_Process == Alter_select[3]:
        data_retention_time_in_days = st.text_input(int("Enter no of days to retain"))
    elif Alter_select == Alter_select[2]:
        old_name = st.text_input()
        new_name = st.text_input()
    else:
        pass


##Alter table

    def alter_changes(db_name, schema_name,table_name,Column_Mapping, Userenvironment, User_Login_Environment, Alter_select, Select_alter_Process):
        k = ""
        if User_Login_Environment == Userenvironment[2]:
            if (Select_alter_Process == Alter_select[0]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} add column {Column_Mapping};"
            elif (Select_alter_Process == Alter_select[1]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} drop column {Column_Mapping};"
            elif (Select_alter_Process == Alter_select[2]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} Rename column old_name to new_name;"
            else:
                k = k + "Alter table {table_name} set data_retention_time_in_days=  ;"
            check()
            with open(f'{path}.ddl', 'w') as file:
                # Write content to the file
                file.write(f'{k}')
        elif User_Login_Environment == Userenvironment[0]:
            connection1, cursor1 = Test(selected_user)
            k = ""
            if (Select_alter_Process == Alter_select[0]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} add column {Column_Mapping};"
            elif (Select_alter_Process == Alter_select[1]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} drop column {Column_Mapping};"
            elif (Select_alter_Process == Alter_select[2]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} Rename column old_name to new_name;"
            else:
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} set data_retention_time_in_days=  ;"
            print(k)
            cursor1.execute(k)
        else:
            connection, cursor = IT(selected_user)

            if (Select_alter_Process == Alter_select[0]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} add column {Column_Mapping};"
            elif (Select_alter_Process == Alter_select[1]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} drop column {(Column_Mapping)};"
            elif (Select_alter_Process == Alter_select[2]):
                k = k + f"Alter table {db_name}.{schema_name}.{table_name} Rename column {old_name} to {new_name};"
            else:
                k = k + "Alter table {table_name} set data_retention_time_in_days=  ;"
            print(k)
            cursor.execute(k)

    if st.button("Create Alter", key="create_table_button"):
        if Enter_Table_name and Column_Mapping:
            alter_changes(Select_Database_Name,Select_Schema_Name,Enter_Table_name, Column_Mapping, Userenvironment, User_Login_Environment, Alter_select, Select_alter_Process)
            st.success(f"Table '{Enter_Table_name}' created successfully!")
        else:
            st.error("Please enter a table name, select a prefix, and provide at least one column.")



def main():
    st.sidebar.title("Navigation")
    pages = {"object_management_page": home_page, "Object_Alter_Page":another_page}

    selected_page = st.sidebar.selectbox("Select a page", list(pages.keys()))

    # Display the selected page
    pages[selected_page]()

if __name__ == "__main__":
    main()

