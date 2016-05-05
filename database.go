package yue

import (
	"log"
	"fmt"
	"strings"
	"database/sql"

	proto "github.com/umegaya/yue/proto"

	_ "github.com/cockroachdb/pq"
	"github.com/umegaya/gorp"
)

type database struct {
	gorp.DbMap
	node *Node
}
type dbh interface {
	Delete(list ...interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Insert(list ...interface{}) error
	Prepare(query string) (*sql.Stmt, error)
	Select(i interface{}, query string, args ...interface{}) ([]interface{}, error)
	SelectOne(holder interface{}, query string, args ...interface{}) error
	Update(list ...interface{}) (int64, error)
	UpdateColumns(filter gorp.ColumnFilter, list ...interface{}) (int64, error)	
}
type txn struct {
	*gorp.Transaction
}

func newdatabase() *database {
	//setup database
	return &database{gorp.DbMap{Db: nil, Dialect: gorp.NewCockroachDialect()}, nil}
} 

func (dbm *database) init(c *Config) error {
	url := fmt.Sprintf("postgresql://root@%s:26257?sslmode=disable&connect_timeout=%d", 
		c.DatabaseAddress, c.MeshServerConnTimeoutSec)
	if len(c.CertPath) > 0 {
		url = fmt.Sprintf(
			"postgresql://root@%s:26257?sslmode=verify-full&sslcert=%s&sslrootcert=%s&sslkey=%s&connect_timeout=%d", 
			c.DatabaseAddress, 
			fmt.Sprint("%s/ca.crt", c.CertPath), 
			fmt.Sprintf("%s/root.client.crt", c.CertPath), 
			fmt.Sprintf("%s/ca.key", c.CertPath),
			c.MeshServerConnTimeoutSec)
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return err
	}
	dbm.Db = db
	table(Node{}, "nodes", "Id").AddIndex("addr", "UNIQUE", []string{"Address"})
	table(proto.Actor{}, "actors", "Id")
	log.Printf("initialize actor store")
	//create table according to model definition. TODO: how to do migration with roach?
    if err := dbm.CreateTablesIfNotExists(); err != nil {
    	log.Printf("CreateTablesIfNotExists: %v", err)
    	return err
    }
    log.Printf("create actor tables")
    if _, err := dbm.Exec("set database = yue;"); err != nil {
    	log.Printf("exec set database: %v", err)
    	return err
    }
    if err := dbm.CreateIndex(); err != nil {
    	if strings.Contains(err.Error(), "duplicate index name") {
    		log.Printf("index already created: ignore error %v", err)
    	} else {
	    	log.Printf("CreateIndex: %v", err)
    		return err    	
    	}
    }
    log.Printf("create actor indexes")
    //initialize node object
    dbm.node, _, err = newnode(dbm, c.HostAddress)
    if err != nil {
    	log.Printf("NewNode: %v", err)
    	return err
    }
    log.Printf("end bootstrap actor storage")
    return nil
}

func (dbm *database) txn(fn func (dbh) error) error {
	tx, err := dbm.Begin()
	if err != nil {
		return err
	}
	txn := txn { tx }
	if err := fn(txn); err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func store(dbh dbh, record interface {}, columns []string) (int64, error) {
	return dbh.UpdateColumns(func (col *gorp.ColumnMap) bool {
		for _, c := range columns {
			if col.ColumnName == c {
				return true
			}
		}
		return false
	}, record)
}

func table(tmpl interface {}, name string, pkey_column string) *gorp.TableMap {
	return db().AddTableWithNameAndSchema(tmpl, "yue", name).SetKeys(false, pkey_column)
}
