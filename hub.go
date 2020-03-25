package datahub

import (
	"fmt"
	"sync"

	"git.eaciitapp.com/sebar/dbflex"
	"git.eaciitapp.com/sebar/dbflex/orm"
)

type Hub struct {
	connFn   func() (dbflex.IConnection, error)
	usePool  bool
	pool     *dbflex.DbPooling
	poolSize int

	poolItems []*dbflex.PoolItem
	mtx       *sync.Mutex
}

func NewHub(fn func() (dbflex.IConnection, error), usePool bool, poolsize int) *Hub {
	h := new(Hub)
	h.connFn = fn
	h.usePool = usePool
	h.poolSize = poolsize
	return h
}

func (h *Hub) getConnFromPool() (int, dbflex.IConnection, error) {
	if h.poolSize == 0 {
		h.poolSize = 100
	}

	if h.mtx == nil {
		h.mtx = new(sync.Mutex)
	}

	if h.pool == nil {
		h.pool = dbflex.NewDbPooling(h.poolSize, h.connFn)
	}

	it, err := h.pool.Get()
	if err != nil {
		return -1, nil, fmt.Errorf("unable get connection from pool. %s", err.Error())
	}

	conn := it.Connection()
	idx := -1
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.poolItems = append(h.poolItems, it)
	idx = len(h.poolItems) - 1
	return idx, conn, nil
}

func (h *Hub) closeConn(idx int, conn dbflex.IConnection) {
	if !h.usePool {
		conn.Close()
	}

	if idx < len(h.poolItems) && idx != -1 {
		if h.mtx == nil {
			h.mtx = new(sync.Mutex)
		}

		h.mtx.Lock()
		defer h.mtx.Unlock()

		itemCount := len(h.poolItems)
		h.poolItems[idx].Release()

		if itemCount == 0 {
			h.poolItems = []*dbflex.PoolItem{}
		} else if idx == 0 {
			h.poolItems = h.poolItems[1:]
		} else if idx == len(h.poolItems)-1 {
			h.poolItems = h.poolItems[:idx]
		} else {
			h.poolItems = append(h.poolItems[:idx], h.poolItems[idx+1:]...)
		}
	}
}

func (h *Hub) getConn() (int, dbflex.IConnection, error) {
	if h.connFn == nil {
		return -1, nil, fmt.Errorf("connection fn is not yet defined")
	}

	if h.usePool {
		return h.getConnFromPool()
	}

	conn, err := h.connFn()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to open connection. %s", err.Error())
	}
	return -1, conn, nil
}

func (h *Hub) UsePool() bool {
	return h.usePool
}

func (h *Hub) PoolSize() int {
	return h.poolSize
}

func (h *Hub) DeleteQuery(model orm.DataModel, where *dbflex.Filter) error {
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	cmd := dbflex.From(model.TableName()).Delete()
	if where != nil {
		cmd.Where(where)
	}
	_, err = conn.Execute(cmd, nil)
	return err
}

func (h *Hub) Save(data orm.DataModel) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Save(conn, data); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Insert(data orm.DataModel) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Insert(conn, data); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Update(data orm.DataModel) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Update(conn, data); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Delete(data orm.DataModel) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Delete(conn, data); err != nil {
		return err
	}

	return nil
}

/*
func (h *Hub) Cursor(data orm.DataModel, parm dbflex.QueryParam) (dbflex.ICursor, error) {
	idx, conn, err := h.getConn()
	if err != nil {
		return nil, fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	qry := dbflex.From(data.TableName())
	if w := parm.Where; w != nil {
		qry.Where(w)
	}
	if o := parm.Sort; len(o) > 0 {
		qry.OrderBy(o...)
	}
	if o := parm.Skip; o > 0 {
		qry.Skip(o)
	}
	if o := parm.Take; o > 0 {
		qry.Take(o)
	}
	cur := conn.Cursor(qry, nil)
	if cur.Error() != nil {
		cur.Close()
		return nil, fmt.Errorf("cursor error. %s", cur.Error().Error())
	}
	return cur, nil
}
*/

func (h *Hub) GetByID(data orm.DataModel, ids ...interface{}) error {
	data.SetThis(data)
	data.SetID(...ids)
	return h.Get(data)
}

func (h *Hub) Get(data orm.DataModel) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Get(conn, data); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Gets(data orm.DataModel, parm *dbflex.QueryParam, dest interface{}) error {
	if parm == nil {
		parm = dbflex.NewQueryParam()
	}

	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	if err = orm.Gets(conn, data, dest, parm); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Count(data orm.DataModel, qp *dbflex.QueryParam) (int, error) {
	if qp == nil {
		qp = dbflex.NewQueryParam()
	}

	idx, conn, err := h.getConn()
	if err != nil {
		return 0, fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	var cmd dbflex.ICommand
	if qp == nil || qp.Where == nil {
		cmd = dbflex.From(data.TableName())
	} else {
		cmd = dbflex.From(data.TableName()).Where(qp.Where)
	}
	cur := conn.Cursor(cmd, nil)
	if err = cur.Error(); err != nil {
		return 0, fmt.Errorf("cursor error. %s", err.Error())
	}
	return cur.Count(), nil
}

func (h *Hub) Aggregate(data orm.DataModel, parm *dbflex.QueryParam, dest interface{}) error {
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	qry := dbflex.From(data.TableName())
	if w := parm.Where; w != nil {
		qry.Where(w)
	}
	if o := parm.Sort; len(o) > 0 {
		qry.OrderBy(o...)
	}
	if o := parm.Skip; o > 0 {
		qry.Skip(o)
	}
	if o := parm.Take; o > 0 {
		qry.Take(o)
	}
	if o := parm.GroupBy; len(o) > 0 {
		qry.GroupBy(o...)
	}
	if o := parm.Aggregates; len(o) > 0 {
		qry.Aggr(o...)
	}

	cur := conn.Cursor(qry, nil)
	if err = cur.Error(); err != nil {
		return fmt.Errorf("error when running cursor for aggregation. %s", err.Error())
	}
	defer cur.Close()

	err = cur.Fetchs(dest, 0)
	return err
}

func (h *Hub) Close() {
	if h.usePool {
		h.pool.Close()
	}
}
