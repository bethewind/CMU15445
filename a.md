##

### Column

Column contain the meta information of a column. It contain the following information:

- column name
- column type
- fixed length
- variable length
- AbstractExpression
- column offset : column offset in the tuple.

if this column is a inline column, the variable length is zero.
if the column is a noinline column, the fix length is the size of pointer.
It stores a pointer and the pointer point to other place where the real data is stored.

The column offset is compute in the constructor of Schema, it accumulate the fix length of each column.

## Schema

Schema represent the meta information of a table(The all column)

- length_ : the sum of fix length of all column.
- columns_ 
- tuple_is_inlines_: whether all columnis inline.
- uninlined_columns_: the index of all uninlined columns.


## tuple

tuple represent the real data.
if the column is inline. then the data is stored.
if the column is noinline. store the offset of this field. then the offset location store the length of variable data and real data. so the room of variable column is the size of offset + the length of variable data and the size of real data.


## TableHeap

TableHeap represent a physical table on disk, it is just a doubly-linked list of TablePage

## TablePage

The TablePage is the memory container of physical Page.
The format is as following


 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  ----------------------------------------------------------------


