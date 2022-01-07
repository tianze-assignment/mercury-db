#include "MyVisitor.h"

#include <cstdio>

const char *string_to_char(std::string s) {
    return strdup(s.c_str());
}

MyVisitor::MyVisitor(DBManager *db_manager, TableManager *table_manager) : db_manager(db_manager), table_manager(table_manager) {}

antlrcpp::Any MyVisitor::visitProgram(SQLParser::ProgramContext *context) {
    for (auto statement : context->statement())
        results.push_back(statement->accept(this));
    return antlrcpp::Any(results);
}

antlrcpp::Any MyVisitor::visitStatement(SQLParser::StatementContext *context) {
    if (auto s = context->db_statement()) return s->accept(this);
    if (auto s = context->io_statement()) return s->accept(this);
    if (auto s = context->table_statement()) return s->accept(this);
    if (auto s = context->alter_statement()) return s->accept(this);
    if (auto s = context->Annotation()) return s->accept(this);
    if (auto s = context->Null()) return s->accept(this);
    return antlrcpp::Any(-1);
}

antlrcpp::Any MyVisitor::visitCreate_db(SQLParser::Create_dbContext *context) {
    std::string name = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(this->db_manager->create_db(name)));
}

antlrcpp::Any MyVisitor::visitDrop_db(SQLParser::Drop_dbContext *context) {
    std::string name = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(this->db_manager->drop_db(name)));
}

antlrcpp::Any MyVisitor::visitShow_dbs(SQLParser::Show_dbsContext *context) {
    return antlrcpp::Any(string_to_char(this->db_manager->show_dbs()));
}

antlrcpp::Any MyVisitor::visitUse_db(SQLParser::Use_dbContext *context) {
    std::string name = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(this->db_manager->use_db(name)));
}

antlrcpp::Any MyVisitor::visitShow_tables(SQLParser::Show_tablesContext *context) {
    return antlrcpp::Any(string_to_char(this->db_manager->show_tables()));
}

antlrcpp::Any MyVisitor::visitShow_indexes(SQLParser::Show_indexesContext *context) {
    // show all idnexed of database ?
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitLoad_data(SQLParser::Load_dataContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitDump_data(SQLParser::Dump_dataContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitCreate_table(SQLParser::Create_tableContext *context) {
    string table_name = context->Identifier()->getText();
    context->field_list()->accept(this);
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitDrop_table(SQLParser::Drop_tableContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitDescribe_table(SQLParser::Describe_tableContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitUpdate_table(SQLParser::Update_tableContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSelect_table_(SQLParser::Select_table_Context *context) {
    return context->select_table()->accept(this);
}

antlrcpp::Any MyVisitor::visitSelect_table(SQLParser::Select_tableContext *context) {
    return antlrcpp::Any("This is a select statement!");
}

antlrcpp::Any MyVisitor::visitAlter_add_index(SQLParser::Alter_add_indexContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_drop_index(SQLParser::Alter_drop_indexContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitField_list(SQLParser::Field_listContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitNormal_field(SQLParser::Normal_fieldContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitType_(SQLParser::Type_Context *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitValue_lists(SQLParser::Value_listsContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitValue_list(SQLParser::Value_listContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitValue(SQLParser::ValueContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_operator_select(SQLParser::Where_operator_selectContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_null(SQLParser::Where_nullContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_in_list(SQLParser::Where_in_listContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_in_select(SQLParser::Where_in_selectContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitWhere_like_string(SQLParser::Where_like_stringContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitColumn(SQLParser::ColumnContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitExpression(SQLParser::ExpressionContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSet_clause(SQLParser::Set_clauseContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSelectors(SQLParser::SelectorsContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSelector(SQLParser::SelectorContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitIdentifiers(SQLParser::IdentifiersContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitOperator_(SQLParser::Operator_Context *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitAggregator(SQLParser::AggregatorContext *context) {
    return antlrcpp::Any(0);
}
