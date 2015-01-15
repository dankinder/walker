package cassandra

import "github.com/iParadigms/walker"

// PriorityURL is a heap of URLs, where the next element Pop'ed off the list
// points to the oldest (as measured by LastCrawled) element in the list. This
// class is designed to be used with the container/heap package. This type is
// currently only used in generateSegments
type PriorityURL []*walker.URL

// Returns the length of this PriorityURL
func (pq PriorityURL) Len() int {
	return len(pq)
}

// Return logical less-than between two items in this PriorityURL
func (pq PriorityURL) Less(i, j int) bool {
	return pq[i].LastCrawled.Before(pq[j].LastCrawled)
}

// Swap two items in this PriorityURL
func (pq PriorityURL) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push an item onto this PriorityURL
func (pq *PriorityURL) Push(x interface{}) {
	*pq = append(*pq, x.(*walker.URL))
}

// Pop an item onto this PriorityURL
func (pq *PriorityURL) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}
