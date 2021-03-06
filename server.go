package std

import (
	"context"
	"sync"

	"github.com/aclivo/olap"
)

type server struct {
	olap.Storage
	sync.RWMutex
}

func NewServer(storage olap.Storage, delay int64) olap.Server {
	return &server{
		Storage: storage,
	}
}

func (s *server) Get(ctx context.Context, cube string, elements ...string) float64 {
	return s.get(ctx, cube, elements...)
}

func (s *server) Put(ctx context.Context, value float64, cube string, elements ...string) {
	s.put(ctx, value, cube, elements...)
}

func (s *server) ExecuteProcess(ctx context.Context, name string) error {
	return nil
}

func (s *server) GetStorage(ctx context.Context) (olap.Storage, error) {
	return s.getStorage()
}

func (s *server) NewView(ctx context.Context, cube string, elements ...[]string) (olap.View, error) {
	return s.newView(ctx, cube, elements...)
}

func (s *server) Query(ctx context.Context, view olap.View) (olap.Rows, error) {
	return s.query(ctx, view)
}

func (s *server) get(ctx context.Context, cube string, elements ...string) float64 {
	if c, err := s.Storage.GetCell(ctx, cube, elements...); err == nil {
		return c.Value
	}
	if isCon, _ := s.isConsolidatedCell(ctx, cube, elements...); isCon {
		cube, _ := s.GetCube(ctx, cube)
		dimElements, _ := s.expand(ctx, cube, elements...)
		cart := cartesian(dimElements)
		var ret float64
		for _, cslc := range cart {
			x := s.Get(ctx, cube.Name, cslc...)
			ret += x
		}
		return ret
	}
	// else {
	//	for _, r := range s.rules[cube] {
	//		if r.Match(ctx, elements...) {
	//			return r.Eval(ctx, elements...), nil
	//		}
	//	}
	//}
	panic("fatal")
}

func (s *server) put(ctx context.Context, value float64, cube string, elements ...string) {
	s.Storage.AddCell(ctx, olap.Cell{Cube: cube, Elements: elements, Value: value})
}

func (s *server) getStorage() (olap.Storage, error) {
	return s.Storage, nil
}

func (s *server) newView(ctx context.Context, cube string, elements ...[]string) (olap.View, error) {
	s.RLock()
	defer s.RUnlock()
	cub, _ := s.GetCube(ctx, cube)
	if len(cub.Dimensions) != len(elements) {
		panic("dimension mismatch")
	}
	slices := map[string][]string{}
	for k, d := range cub.Dimensions {
		slices[d] = elements[k]
	}
	return olap.View{Cube: cube, Slices: slices}, nil
}

func (s *server) query(ctx context.Context, view olap.View) (olap.Rows, error) {
	cube, _ := s.GetCube(ctx, view.Cube)
	slcels := [][]string{}
	for _, dname := range cube.Dimensions {
		slcels = append(slcels, view.Slices[dname])
	}
	comb := cartesian(slcels)
	columns := []string{}
	columns = append(columns, "value")
	rs := &rows{
		server:  s,
		columns: columns,
		view:    &view,
		combs:   comb,
	}
	return rs, nil
}

func (s *server) AddCell(ctx context.Context, cell olap.Cell) error {
	panic("method not allowed")
}

func (s *server) GetCellByName(ctx context.Context, cube string, elements ...string) (olap.Cell, error) {
	panic("method not allowed")
}

func (s *server) expand(ctx context.Context, cube olap.Cube, elementName ...string) ([][]string, error) {
	dimElements := [][]string{}
	for k := range elementName {
		ex, err := s.Storage.GetElement(ctx, cube.Dimensions[k], elementName[k])
		if err != nil {
			return [][]string{}, err
		}
		elements := []string{}
		if _, err := s.GetComponent(ctx, ex.Dimension, ex.Name); err != nil {
			elements = append(elements, ex.Name)
		} else {
			chs, _ := s.Children(ctx, ex.Dimension, ex.Name)
			for _, t := range chs {
				elements = append(elements, t.Name)
			}
		}
		dimElements = append(dimElements, []string{})
		dimElements[k] = elements
	}
	return dimElements, nil
}

func (s *server) isConsolidatedCell(ctx context.Context, cub string, elements ...string) (bool, error) {
	cube, err := s.GetCube(ctx, cub)
	if err != nil {
		return false, err
	}
	var ret bool
	for k, dname := range cube.Dimensions {
		if _, err := s.GetComponent(ctx, dname, elements[k]); err != nil {
			return false, err
		}
		ret = ret || true
	}
	return ret, nil
}

type rows struct {
	server  olap.Server
	columns []string
	view    *olap.View
	combs   [][]string
	ref     int
}

func (rs *rows) Columns() []string {
	return rs.columns
}

func (rs *rows) Next() bool {
	cnt := rs.ref < len(rs.combs)
	return cnt
}

func (rs *rows) Scan(dest ...interface{}) {
	for x := 0; x < len(rs.view.Slices); x++ {
		d := dest[x].(*string)
		*d = rs.combs[rs.ref][x]
	}
	// TODO: Remove
	ctx := context.Background()
	v := rs.server.Get(ctx, rs.view.Cube, rs.combs[rs.ref]...)
	d := dest[len(rs.view.Slices)].(*float64)
	*d = v
	rs.ref++
}

// Cartesian product of the elements provided
//    Ex: [[a b] [c d]] => [[a c] [a d] [b c] [b d]]
func cartesian(els [][]string) [][]string {
	c := 1
	for _, a := range els {
		c *= len(a)
	}
	if c == 0 {
		return nil
	}
	result := make([][]string, c)
	b := make([]string, c*len(els))
	n := make([]int, len(els))
	x := 0
	for i := range result {
		e := x + len(els)
		pi := b[x:e]
		result[i] = pi
		x = e
		for j, n := range n {
			pi[j] = els[j][n]
		}
		for j := len(n) - 1; j >= 0; j-- {
			n[j]++
			if n[j] < len(els[j]) {
				break
			}
			n[j] = 0
		}
	}
	return result
}
