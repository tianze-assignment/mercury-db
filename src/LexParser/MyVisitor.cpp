#include "MyVisitor.h"

#include <cstdio>
#include <exception>
#include <ctime>

#include "Schema.h"
#include "Query.h"

const char *string_to_char(std::string s) {
    return strdup(s.c_str());
}

MyVisitor::MyVisitor(DBManager *db_manager) : db_manager(db_manager) {}

antlrcpp::Any MyVisitor::visitProgram(SQLParser::ProgramContext *context) {
    for (auto statement : context->statement()) {
        try {
            results.push_back(statement->accept(this));
        }
        catch(DBException e) {
            results.push_back(string_to_char(e.what()));
        }
    }
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
    string filename = context->String()->getText();
    filename.erase(0, 1); filename.pop_back();
    string table = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(
        db_manager->load_data(filename, table)
    ));
}

antlrcpp::Any MyVisitor::visitDump_data(SQLParser::Dump_dataContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitCreate_table(SQLParser::Create_tableContext *context) {
    Schema schema;
    schema.table_name = context->Identifier()->getText();
    for (auto field : context->field_list()->field()) {
        auto r = field->accept(this);
        try {
            auto column = r.as<Column>();
            schema.columns.push_back(column);
        } catch (bad_cast) {
            try {
                auto pk = r.as<PK>();
                schema.pk = pk;
            } catch (bad_cast) {
                auto fk = r.as<FK>();
                schema.fks.push_back(fk);
            }
        }
    }
    return antlrcpp::Any(string_to_char(
        this->db_manager->create_table(schema)));
}

antlrcpp::Any MyVisitor::visitDrop_table(SQLParser::Drop_tableContext *context) {
    string name = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(
        this->db_manager->drop_table(name)
    ));
}

antlrcpp::Any MyVisitor::visitDescribe_table(SQLParser::Describe_tableContext *context) {
    string name = context->Identifier()->getText();
    return antlrcpp::Any(string_to_char(
        this->db_manager->describe_table(name)
    ));
}

antlrcpp::Any MyVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *context) {
    string table_name = context->Identifier()->getText();
    auto value_lists = context->value_lists()->accept(this).as<vector<vector<Value>>>();
    return antlrcpp::Any(string_to_char(
        this->db_manager->insert(table_name, value_lists)
    ));
}

antlrcpp::Any MyVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext *context) {
    string table_name = context->Identifier()->getText();
    auto conditions = context->where_and_clause()->accept(this).as<vector<Condition>>();
    return antlrcpp::Any(string_to_char(
        this->db_manager->delete_(table_name, conditions)
    ));
}

antlrcpp::Any MyVisitor::visitUpdate_table(SQLParser::Update_tableContext *context) {
    string table_name = context->Identifier()->getText();
    auto assignments = context->set_clause()->accept(this).as<vector<pair<string,Value>>>();
    auto conditions = context->where_and_clause()->accept(this).as<vector<Condition>>();
    return antlrcpp::Any(string_to_char(
        this->db_manager->update(table_name, assignments, conditions)
    ));
}

antlrcpp::Any MyVisitor::visitSelect_table_(SQLParser::Select_table_Context *context) {
    clock_t start = clock();
    auto query = context->select_table()->accept(this).as<Query>();
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    string res = query.to_str() + "\n";
    int rows = query.value_lists.size();
    res += DBManager::rows_text(rows) + " in set (" + to_string(use_time) + " Sec)";
    return antlrcpp::Any(string_to_char(res));
}

antlrcpp::Any MyVisitor::visitSelect_table(SQLParser::Select_tableContext *context) {
    vector<QueryCol> cols;
    Aggregator agg;
    bool has_normal = false;
    for (auto selector: context->selectors()->selector()) {
        if (!selector->column()) {
            agg.ops.push_back(CNT_);
            continue;
        }
        cols.push_back(selector->column()->accept(this).as<QueryCol>());
        if (auto ag = selector->aggregator()) agg.ops.push_back(ag->accept(this).as<Aggregator_OP>());
        else has_normal = true;
    }
    if (!agg.ops.empty() && has_normal) throw DBException("Can not select aggregators and normal rows in one query");
    if (auto col = context->column()) {
        agg.group_by = col->accept(this).as<QueryCol>();
        cols.push_back(agg.group_by);
    }
    if (!agg.group_by.second.empty() && agg.ops.empty()) throw DBException("\"GROUP BY\" should use with aggregators");
    auto tables = context->identifiers()->accept(this).as<vector<string>>();
    vector<Condition> conds;
    if (auto c = context->where_and_clause()) conds = c->accept(this).as<vector<Condition>>();
    int limit = -1, offset = 0;
    if (context->Integer().size() > 0) limit = stoi(context->Integer()[0]->getText());
    if (context->Integer().size() > 1) offset = stoi(context->Integer()[1]->getText());
    return antlrcpp::Any(db_manager->select(cols, tables, conds, agg, limit, offset));
}

antlrcpp::Any MyVisitor::visitAlter_add_index(SQLParser::Alter_add_indexContext *context) {
    string table_name = context->Identifier()->getText();
    vector<string> ids = context->identifiers()->accept(this).as<vector<string>>();
    return antlrcpp::Any(string_to_char(
        db_manager->alter_add_index(table_name, ids)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_drop_index(SQLParser::Alter_drop_indexContext *context) {
    string table_name = context->Identifier()->getText();
    vector<string> ids = context->identifiers()->accept(this).as<vector<string>>();
    return antlrcpp::Any(string_to_char(
        db_manager->alter_drop_index(table_name, ids)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *context) {
    string table_name = context->Identifier()[0]->getText();
    string pk_name;
    if(context->Identifier().size() > 1) pk_name = context->Identifier()[1]->getText();
    return antlrcpp::Any(string_to_char(
        db_manager->alter_drop_pk(table_name, pk_name)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *context) {
    string table_name = context->Identifier()[0]->getText();
    string fk_name = context->Identifier()[1]->getText();
    return antlrcpp::Any(string_to_char(
        db_manager->alter_drop_fk(table_name, fk_name)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *context) {
    string table_name = context->Identifier()[0]->getText();
    string pk_name = context->Identifier()[1]->getText();
    vector<string> pks;
    for(auto i : context->identifiers()->Identifier())
        pks.push_back(i->getText());
    return antlrcpp::Any(string_to_char(
        db_manager->alter_add_pk(table_name, pk_name, pks)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *context) {
    string table_name = context->Identifier()[0]->getText();
    string fk_name = context->Identifier()[1]->getText();
    string ref_table_name = context->Identifier()[2]->getText();
    vector<string> fields = context->identifiers()[0]->accept(this).as<vector<string>>();
    vector<string> ref_fields = context->identifiers()[1]->accept(this).as<vector<string>>();
    return antlrcpp::Any(string_to_char(
        db_manager->alter_add_fk(table_name, fk_name, ref_table_name, fields, ref_fields)
    ));
}

antlrcpp::Any MyVisitor::visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitField_list(SQLParser::Field_listContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitNormal_field(SQLParser::Normal_fieldContext *context) {
    Column column;
    column.name = context->Identifier()->getText();
    // type
    if (auto i = context->type_()->Integer()) column.varchar_len = std::stoi(i->getText());
    string type = context->type_()->getText();
    if (type.starts_with("INT")) column.type = INT;
    if (type.starts_with("VARCHAR")) column.type = VARCHAR;
    if (type.starts_with("FLOAT")) column.type = FLOAT;
    // not null
    column.not_null = context->Null() != nullptr;
    // default
    if (auto v = context->value()) {
        column.has_default = true;
        column.default_value = v->accept(this).as<Value>();
    } else {  // if no default
        column.has_default = false;
    }
    return antlrcpp::Any(column);
}

antlrcpp::Any MyVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext *context) {
    PK pk;
    if (auto i = context->Identifier()) pk.name = i->getText();
    for (auto i : context->identifiers()->Identifier()) pk.pks.push_back(i->getText());
    return antlrcpp::Any(pk);
}

antlrcpp::Any MyVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext *context) {
    FK fk;
    if (auto is = context->Identifier(); is.size() == 1) {
        fk.ref_table = is[0]->getText();
    } else {
        fk.name = is[0]->getText();
        fk.ref_table = is[1]->getText();
    }
    auto iss = context->identifiers();
    for (auto is : iss[0]->Identifier()) fk.fks.push_back(is->getText());
    for (auto is : iss[1]->Identifier()) fk.ref_fks.push_back(is->getText());
    return antlrcpp::Any(fk);
}

antlrcpp::Any MyVisitor::visitType_(SQLParser::Type_Context *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitValue_lists(SQLParser::Value_listsContext *context) {
    vector<vector<Value>> value_lists;
    for(auto value_list : context->value_list()){
        value_lists.push_back(value_list->accept(this).as<vector<Value>>());
    }
    return antlrcpp::Any(value_lists);
}

antlrcpp::Any MyVisitor::visitValue_list(SQLParser::Value_listContext *context) {
    vector<Value> values;
    for(auto value : context->value()){
        values.push_back(value->accept(this).as<Value>());
    }
    return antlrcpp::Any(values);
}

antlrcpp::Any MyVisitor::visitValue(SQLParser::ValueContext *context) {
    Value v;
    if(context->Null()) {
        v.type = NULL_TYPE;
    }else if(auto i = context->Integer()){
        v.type = INT;
        int value = std::stoi(i->getText());
        uint8_t *bytes = static_cast<uint8_t *>(static_cast<void *>(&value));
        v.bytes = vector<uint8_t>(bytes, bytes + 4);
    }else if(auto f = context->Float()){
        v.type = FLOAT;
        float value = std::stof(f->getText());
        uint8_t *bytes = static_cast<uint8_t *>(static_cast<void *>(&value));
        v.bytes = vector<uint8_t>(bytes, bytes + 4);
    }else if(auto s = context->String()) {
        v.type = VARCHAR;
        string value = s->getText();
        value.erase(0, 1); value.pop_back();
        v.bytes = vector<uint8_t>(value.begin(), value.end());
    }
    return antlrcpp::Any(v);
}

antlrcpp::Any MyVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *context) {
    vector<Condition> conditions;
    for(auto condition: context->where_clause())
        conditions.push_back(condition->accept(this).as<Condition>());
    return antlrcpp::Any(conditions);
}

antlrcpp::Any MyVisitor::visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = context->operator_()->accept(this).as<CMP_OP>();
    if (auto c = context->expression()->column()) cond.b_col = c->accept(this).as<QueryCol>();
    else cond.b_val = context->expression()->value()->accept(this).as<Value>();
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitWhere_operator_select(SQLParser::Where_operator_selectContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = context->operator_()->accept(this).as<CMP_OP>();
    cond.b_val = context->select_table()->accept(this).as<Query>().to_value();
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitWhere_null(SQLParser::Where_nullContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = IS;
    cond.b_val.type = context->children.size() == 3 ? NULL_TYPE : INT;
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitWhere_in_list(SQLParser::Where_in_listContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = IN;
    cond.b_value_lists.push_back(context->value_list()->accept(this).as<vector<Value>>());
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitWhere_in_select(SQLParser::Where_in_selectContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = IN;
    cond.b_value_lists = context->select_table()->accept(this).as<Query>().value_lists;
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitWhere_like_string(SQLParser::Where_like_stringContext *context) {
    Condition cond;
    cond.a = context->column()->accept(this).as<QueryCol>();
    cond.op = LIKE;
    cond.b_val.type = VARCHAR;
    string s = context->String()->getText();
    cond.b_val.bytes = vector<uint8_t>(s.begin()+1, s.end()-1);
    return antlrcpp::Any(cond);
}

antlrcpp::Any MyVisitor::visitColumn(SQLParser::ColumnContext *context) {
    auto id = context->Identifier();
    return antlrcpp::Any(make_pair(id[0]->getText(), id[1]->getText()));
}

antlrcpp::Any MyVisitor::visitExpression(SQLParser::ExpressionContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSet_clause(SQLParser::Set_clauseContext *context) {
    vector<pair<string,Value>> assignments;
    auto cols = context->Identifier();
    auto values = context->value();
    for (int i = 0; i < cols.size(); ++i)
        assignments.push_back(make_pair(cols[i]->getText(), values[i]->accept(this).as<Value>()));
    return antlrcpp::Any(assignments);
}

antlrcpp::Any MyVisitor::visitSelectors(SQLParser::SelectorsContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitSelector(SQLParser::SelectorContext *context) {
    return antlrcpp::Any(0);
}

antlrcpp::Any MyVisitor::visitIdentifiers(SQLParser::IdentifiersContext *context) {
    vector<string> ids;
    for (auto id: context->Identifier()) ids.push_back(id->getText());
    return antlrcpp::Any(ids);
}

antlrcpp::Any MyVisitor::visitOperator_(SQLParser::Operator_Context *context) {
    CMP_OP op;
    if (context->EqualOrAssign()) op = EQUAL;
    else if (context->Less()) op = LESS;
    else if (context->LessEqual()) op = LESS_EQUAL;
    else if (context->Greater()) op = GREATER;
    else if (context->GreaterEqual()) op = GREATER_EQUAL;
    else op = NOT_EQUAL;
    return antlrcpp::Any(op);
}

antlrcpp::Any MyVisitor::visitAggregator(SQLParser::AggregatorContext *context) {
    Aggregator_OP op;
    if (context->Count()) op = CNT;
    else if (context->Average()) op = AVG;
    else if (context->Max()) op = MAX;
    else if (context->Min()) op = MIN;
    else op = SUM;
    return antlrcpp::Any(op);
}
