package ru.open.cu.student.memory.serializer;

import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.model.HeapTuple;

public interface TupleSerializer {
    <T> HeapTuple serialize(T value, DataType dataType);

    <T> T deserialize(HeapTuple tuple);
}


