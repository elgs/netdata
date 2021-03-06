// dbutils.go
package main

import (
	"database/sql"
	"github.com/elgs/gosqljson"
)

func GeneratePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	var ret string
	for i := 0; i < n; i++ {
		ret += "?,"
	}
	return ret[:len(ret)-1]
}

func GenerateFields(data map[string]interface{}) (string, []interface{}) {
	if len(data) == 0 {
		return "", []interface{}{}
	}
	var fields string
	var values []interface{}
	for k, v := range data {
		fields += k + "=?,"
		values = append(values, v)
	}
	return fields[:len(fields)-1], values
}

func GenerateConditions(conditions map[string]interface{}) (string, []interface{}) {
	if len(conditions) == 0 {
		return "", []interface{}{}
	}
	var fields string
	var values []interface{}
	for k, v := range conditions {
		fields += " AND " + k + "=?"
		values = append(values, v)
	}
	return fields, values
}

func DbInsert(db *sql.DB, table string, data map[string]interface{}, ignore bool, replace bool) (int64, error) {
	fields, values := GenerateFields(data)
	insert := "INSERT INTO "
	if ignore {
		insert = "INSERT IGNORE INTO "
	}
	if replace {
		insert = "REPLACE INTO "
	}
	return gosqljson.ExecDb(db, insert+table+" SET "+fields, values...)
}

func DbUpdate(db *sql.DB, table string, data map[string]interface{},
	conditions map[string]interface{}) (int64, error) {
	fields, values := GenerateFields(data)
	conditionFields, conditionValues := GenerateConditions(conditions)
	values = append(values, conditionValues)
	return gosqljson.ExecDb(db, "UPDATE "+table+" SET "+fields+" WHERE 1=1"+conditionFields, values...)
}

func TxInsert(tx *sql.Tx, table string, data map[string]interface{}, ignore bool, replace bool) (int64, error) {
	fields, values := GenerateFields(data)
	insert := "INSERT INTO "
	if ignore {
		insert = "INSERT IGNORE INTO "
	}
	if replace {
		insert = "REPLACE INTO "
	}
	return gosqljson.ExecTx(tx, insert+table+" SET "+fields, values...)
}

func TxUpdate(tx *sql.Tx, table string, data map[string]interface{},
	conditions map[string]interface{}) (int64, error) {
	fields, values := GenerateFields(data)
	conditionFields, conditionValues := GenerateConditions(conditions)
	values = append(values, conditionValues)
	return gosqljson.ExecTx(tx, "UPDATE "+table+" SET "+fields+" WHERE 1=1"+conditionFields, values...)
}
