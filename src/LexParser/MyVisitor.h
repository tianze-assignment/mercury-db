#pragma once

#include <cstdio>

#include "SQLParser.h"
#include "SQLVisitor.h"
#include "antlr4-runtime.h"

class MyVisitor : public SQLVisitor {
    std::vector<antlrcpp::Any> results;

   public:
    antlrcpp::Any visitProgram(SQLParser::ProgramContext *context) {
        for (auto statement : context->statement())
            results.push_back(statement->accept(this));
        return antlrcpp::Any(results);
    }

    antlrcpp::Any visitStatement(SQLParser::StatementContext *context) {
        if (auto s = context->db_statement()) return s->accept(this);
        if (auto s = context->io_statement()) return s->accept(this);
        if (auto s = context->table_statement()) return s->accept(this);
        if (auto s = context->alter_statement()) return s->accept(this);
        if (auto s = context->Annotation()) return s->accept(this);
        if (auto s = context->Null()) return s->accept(this);
		return antlrcpp::Any(-1);
    }

    antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitShow_tables(SQLParser::Show_tablesContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitShow_indexes(SQLParser::Show_indexesContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitLoad_data(SQLParser::Load_dataContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitDump_data(SQLParser::Dump_dataContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitInsert_into_table(SQLParser::Insert_into_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitDelete_from_table(SQLParser::Delete_from_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitUpdate_table(SQLParser::Update_tableContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitSelect_table_(SQLParser::Select_table_Context *context) {
        return context->select_table()->accept(this);
    }

    antlrcpp::Any visitSelect_table(SQLParser::Select_tableContext *context) {
        return antlrcpp::Any("This is a select statement!");
    }

    antlrcpp::Any visitAlter_add_index(SQLParser::Alter_add_indexContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_drop_index(SQLParser::Alter_drop_indexContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitField_list(SQLParser::Field_listContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitNormal_field(SQLParser::Normal_fieldContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitPrimary_key_field(SQLParser::Primary_key_fieldContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitForeign_key_field(SQLParser::Foreign_key_fieldContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitType_(SQLParser::Type_Context *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitValue_lists(SQLParser::Value_listsContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitValue_list(SQLParser::Value_listContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitValue(SQLParser::ValueContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_and_clause(SQLParser::Where_and_clauseContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_operator_select(SQLParser::Where_operator_selectContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_null(SQLParser::Where_nullContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_in_list(SQLParser::Where_in_listContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_in_select(SQLParser::Where_in_selectContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitWhere_like_string(SQLParser::Where_like_stringContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitColumn(SQLParser::ColumnContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitExpression(SQLParser::ExpressionContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitSet_clause(SQLParser::Set_clauseContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitSelectors(SQLParser::SelectorsContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitSelector(SQLParser::SelectorContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitIdentifiers(SQLParser::IdentifiersContext *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitOperator_(SQLParser::Operator_Context *context) {
        return antlrcpp::Any(0);
    }

    antlrcpp::Any visitAggregator(SQLParser::AggregatorContext *context) {
        return antlrcpp::Any(0);
    }
};