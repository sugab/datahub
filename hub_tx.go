package datahub

import (
	"errors"
	"fmt"
)

// BeginTx create a hub with Transaction. Commit and/or Rollback need to call later on to close the transaction
func (h *Hub) BeginTx() (*Hub, error) {
	conn, e := h.GetClassicConnection()
	if e != nil {
		return nil, fmt.Errorf("fail BeginTransaction: %s", e.Error())
	}
	if !conn.SupportTx() {
		conn.Close()
		return nil, fmt.Errorf("fail BeginTransaction: connection is not supporting transaction")
	}
	if e = conn.BeginTx(); e != nil {
		return nil, fmt.Errorf("fail BeginTransaction: %s", e.Error())
	}

	ht := new(Hub)
	ht.txconn = conn
	ht._log = h._log
	return ht, nil
}

// Commit commits all change into database
func (h *Hub) Commit() error {
	if h.txconn == nil {
		return errors.New("fail Commit: handler has no transactional connection")
	}
	if e := h.txconn.Commit(); e != nil {
		return fmt.Errorf("fail Commit: %s", e.Error())
	}
	return nil
}

// Rollback to reverts back all change into database
func (h *Hub) Rollback() error {
	if h.txconn == nil {
		return errors.New("fail Rollback: handler has no transactional connection")
	}
	if e := h.txconn.RollBack(); e != nil {
		return fmt.Errorf("fail Rollback: %s", e.Error())
	}
	return nil
}

func (h *Hub) IsTx() bool {
	if h.txconn != nil {
		return h.txconn.IsTx()
	}
	return false
}
