#pragma once

#include "DBManager.h"
#include "TableManager.h"
#include "SQLParser.h"
#include "SQLVisitor.h"
#include "antlr4-runtime.h"

class MyVisitor : public SQLVisitor {
    std::vector<antlrcpp::Any> results;
    DBManager *db_manager;
    TableManager *table_manager;

   public:
    MyVisitor(DBManager *db_manager, TableManager *table_manager);

    antlrcpp::Any visitProgram(SQLParser::ProgramContext *context);

    antlrcpp::Any visitStatement(SQLParser::StatementContext *context);

    antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *context);

    antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *context);

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *context);

    antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *context);

    antlrcpp::Any visitShow_tables(SQLParser::Show_tablesContext *context);

    antlrcpp::Any visitShow_indexes(SQLParser::Show_indexesContext *context);

    antlrcpp::Any visitLoad_data(SQLParser::Load_dataContext *context);

    antlrcpp::Any visitDump_data(SQLParser::Dump_dataContext *context);

    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *context);

    antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *context);

    antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *context);

    antlrcpp::Any visitInsert_into_table(SQLParser::Insert_into_tableContext *context);

    antlrcpp::Any visitDelete_from_table(SQLParser::Delete_from_tableContext *context);

    antlrcpp::Any visitUpdate_table(SQLParser::Update_tableContext *context);

    antlrcpp::Any visitSelect_table_(SQLParser::Select_table_Context *context);

    antlrcpp::Any visitSelect_table(SQLParser::Select_tableContext *context);

    antlrcpp::Any visitAlter_add_index(SQLParser::Alter_add_indexContext *context);

    antlrcpp::Any visitAlter_drop_index(SQLParser::Alter_drop_indexContext *context);

    antlrcpp::Any visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *context);

    antlrcpp::Any visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *context);

    antlrcpp::Any visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *context);

    antlrcpp::Any visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *context);

    antlrcpp::Any visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *context);

    antlrcpp::Any visitField_list(SQLParser::Field_listContext *context);

    antlrcpp::Any visitNormal_field(SQLParser::Normal_fieldContext *context);

    antlrcpp::Any visitPrimary_key_field(SQLParser::Primary_key_fieldContext *context);

    antlrcpp::Any visitForeign_key_field(SQLParser::Foreign_key_fieldContext *context);

    antlrcpp::Any visitType_(SQLParser::Type_Context *context);

    antlrcpp::Any visitValue_lists(SQLParser::Value_listsContext *context);

    antlrcpp::Any visitValue_list(SQLParser::Value_listContext *context);

    antlrcpp::Any visitValue(SQLParser::ValueContext *context);

    antlrcpp::Any visitWhere_and_clause(SQLParser::Where_and_clauseContext *context);

    antlrcpp::Any visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *context);

    antlrcpp::Any visitWhere_operator_select(SQLParser::Where_operator_selectContext *context);

    antlrcpp::Any visitWhere_null(SQLParser::Where_nullContext *context);

    antlrcpp::Any visitWhere_in_list(SQLParser::Where_in_listContext *context);

    antlrcpp::Any visitWhere_in_select(SQLParser::Where_in_selectContext *context);

    antlrcpp::Any visitWhere_like_string(SQLParser::Where_like_stringContext *context);

    antlrcpp::Any visitColumn(SQLParser::ColumnContext *context);

    antlrcpp::Any visitExpression(SQLParser::ExpressionContext *context);

    antlrcpp::Any visitSet_clause(SQLParser::Set_clauseContext *context);

    antlrcpp::Any visitSelectors(SQLParser::SelectorsContext *context);

    antlrcpp::Any visitSelector(SQLParser::SelectorContext *context);

    antlrcpp::Any visitIdentifiers(SQLParser::IdentifiersContext *context);

    antlrcpp::Any visitOperator_(SQLParser::Operator_Context *context);

    antlrcpp::Any visitAggregator(SQLParser::AggregatorContext *context);
};