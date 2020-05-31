package datahub

import (
	"fmt"
	"sync"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"

	"github.com/eaciit/toolkit"
)

type Hub struct {
	connFn   func() (dbflex.IConnection, error)
	usePool  bool
	pool     *dbflex.DbPooling
	poolSize int

	poolItems []*dbflex.PoolItem
	mtx       *sync.Mutex
	_log      *toolkit.LogEngine
}

func NewHub(fn func() (dbflex.IConnection, error), usePool bool, poolsize int) *Hub {
	h := new(Hub)
	h.connFn = fn
	h.usePool = usePool
	h.poolSize = poolsize

	if h.usePool {
		h.pool = dbflex.NewDbPooling(h.poolSize, h.connFn).SetLog(h.Log())
		h.pool.Timeout = 7 * time.Second
		h.pool.AutoClose = 5 * time.Second
		//h.pool.AutoRelease = 3 * time.Second
	}
	return h
}

func (h *Hub) Log() *toolkit.LogEngine {
	if h._log == nil {
		h._log = toolkit.NewLogEngine(true, false, "", "", "")
	}
	return h._log
}

func (h *Hub) SetLog(l *toolkit.LogEngine) *Hub {
	h._log = l
	if h.pool != nil {
		h.pool.SetLog(l)
	}
	return h
}

func (h *Hub) GetConnection() (int, dbflex.IConnection, error) {
	return h.getConn()
}

func (h *Hub) CloseConnection(idx int, conn dbflex.IConnection) {
	h.closeConn(idx, conn)
}

func (h *Hub) getConnFromPool() (int, dbflex.IConnection, error) {
	if h.poolSize == 0 {
		h.poolSize = 100
	}

	if h.mtx == nil {
		h.mtx = new(sync.Mutex)
	}

	if h.pool == nil {
		h.pool = dbflex.NewDbPooling(h.poolSize, h.connFn).SetLog(h.Log())
		h.pool.Timeout = 7 * time.Second
		h.pool.AutoClose = 5 * time.Second
		//h.pool.AutoRelease = 3 * time.Second
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

func (h *Hub) SetAutoCloseDuration(d time.Duration) *Hub {
	if h.usePool {
		if h.pool == nil {
			h.pool = dbflex.NewDbPooling(h.poolSize, h.connFn)
		}
		h.pool.AutoClose = d
	}
	return h
}

func (h *Hub) SetAutoReleaseDuration(d time.Duration) *Hub {
	if h.usePool {
		if h.pool == nil {
			h.pool = dbflex.NewDbPooling(h.poolSize, h.connFn)
		}
		h.pool.Timeout = d + time.Duration(5*time.Second)
		h.pool.AutoRelease = d
	}
	return h
}

func (h *Hub) closeConn(idx int, conn dbflex.IConnection) {
	if !h.usePool {
		conn.Close()
	}

	if h.mtx == nil {
		h.mtx = new(sync.Mutex)
	}
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if idx < len(h.poolItems) && idx != -1 {
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

func (h *Hub) UpdateField(data orm.DataModel, where *dbflex.Filter, update toolkit.M) error {
	data.SetThis(data)
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	updatedFields := []string{}
	for k, _ := range update {
		updatedFields = append(updatedFields, k)
	}

	cmd := dbflex.From(data.TableName()).Update(updatedFields...).Where(where)
	conn.Execute(cmd, toolkit.M{}.Set("data", data))
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
	data.SetID(ids...)
	return h.Get(data)
}

func (h *Hub) GetByParm(data orm.DataModel, parm *dbflex.QueryParam) error {
	data.SetThis(data)
	if parm == nil {
		parm = dbflex.NewQueryParam()
	}

	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	cmd := dbflex.From(data.TableName())
	if len(parm.Select) == 0 {
		cmd.Select()
	} else {
		cmd.Select(parm.Select...)
	}
	if where := parm.Where; where != nil {
		cmd.Where(where)
	}
	if sort := parm.Sort; len(sort) > 0 {
		cmd.OrderBy(sort...)
	}
	if skip := parm.Skip; skip > 0 {
		cmd.Skip(skip)
	}
	if take := parm.Take; take > 0 {
		cmd.Take(take)
	}
	cursor := conn.Cursor(cmd, nil)
	if err := cursor.Error(); err != nil {
		return err
	}
	defer cursor.Close()
	if err = cursor.Fetch(data); err != nil {
		return err
	}
	return nil
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

func (h *Hub) Execute(cmd dbflex.ICommand, parm toolkit.M) (interface{}, error) {
	idx, conn, err := h.getConn()
	if err != nil {
		return nil, fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	return conn.Execute(cmd, parm)
}

func (h *Hub) Populate(cmd dbflex.ICommand, parm toolkit.M, result interface{}) (int, error) {
	idx, conn, err := h.getConn()
	if err != nil {
		return 0, fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	c := conn.Cursor(cmd, parm)
	if err = c.Error(); err != nil {
		return 0, fmt.Errorf("unable to prepare cursor. %s", err.Error())
	}
	if err = c.Fetchs(result, 0); err != nil {
		return 0, fmt.Errorf("unable to fetch data. %s", err.Error())
	}

	return c.Count(), nil
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

func (h *Hub) SaveObject(name string, object interface{}) error {
	idx, conn, err := h.getConn()
	if err != nil {
		return fmt.Errorf("connection error. %s", err.Error())
	}
	defer h.closeConn(idx, conn)

	cmd := dbflex.From(name).Save()
	if _, err = conn.Execute(cmd, toolkit.M{}.Set("data", object)); err != nil {
		return fmt.Errorf("unable to save. %s", err.Error())
	}
	return nil
}
