package ecu

import (
	"sync"
)

const VarsSliceSize = 4096 * 8

var (
	boolSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, VarsSliceSize)
		},
	}
	intSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]int64, VarsSliceSize)
		},
	}
	floatAvgSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]floatAvgValue, VarsSliceSize)
		},
	}
	floatSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]float64, VarsSliceSize)
		},
	}
	strSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, VarsSliceSize)
		},
	}

	intAvgSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]intAvgValue, VarsSliceSize)
		},
	}
	distinctSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]distinctValue, VarsSliceSize)
		},
	}
)
