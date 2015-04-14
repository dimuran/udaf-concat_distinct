/*
 * Copyright (C) 2014 Richárd Ernő Kiss
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package hu.radoop.udaf;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * These methods establish the processing semantics followed by the UDAF
 *
 * @author Richárd Ernő Kiss
 */
public class GenericUDAFConcatDistinctEvaluator extends GenericUDAFEvaluator {

    private static final String outputDelimeter = ", ";

    //ObjectInspectors to serialize-deserialize data
    //For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private transient PrimitiveObjectInspector inputOI;
    //For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations input(list of objs)
    private transient StandardListObjectInspector internalMergeOI;

    /**
     * Initializes object inspectors. Need to return the <b>output</b>
     * ObejctInspector.
     * <p>
     * GenericUDAF mode cheat sheet:<br>
     * COMPLETE: from original data directly to full aggregation: iterate() and
     * terminate() will be called.<br>
     * FINAL: from partial aggregation to full aggregation: merge() and
     * terminate() will be called.<br>
     * PARTIAL1: from original data to partial aggregation data: iterate() and
     * terminatePartial() will be called.<br>
     * PARTIAL2: from partial aggregation data to partial aggregation data:
     * merge() and terminatePartial() will be called.<br>
     *
     * @param m the GenericUDAF mode
     * @param parameters the input
     * @return output ObjectInspector
     * @throws HiveException
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
        super.init(m, parameters);

        if (m == Mode.PARTIAL2 || m == Mode.FINAL) { //input list
            internalMergeOI = (StandardListObjectInspector) parameters[0];
            inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
        } else {//input primitve (PARTIAL1 and COMPLETE )
            inputOI = (PrimitiveObjectInspector) parameters[0];
        }
        if (m == Mode.COMPLETE || m == Mode.FINAL) {//output string
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        } else if (m == Mode.PARTIAL1) {//output list
            return ObjectInspectorFactory
                    .getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(inputOI));
        } else {//output list (PARTIAL2)
            return (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
        }
    }

    static class ConcatDistinctAggregationBuffer implements AggregationBuffer {

        Collection<Object> container = new ArrayList<>();
    }

    /**
     * Returns an object that will be used to store temporary aggregation
     * results.
     *
     * @return @throws HiveException
     */
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        ConcatDistinctAggregationBuffer ret = new ConcatDistinctAggregationBuffer();
        return ret;
    }

    /**
     * Resets the AggregationBuffer.
     *
     * @param ab the AggregationBuffer
     * @throws HiveException
     */
    @Override
    public void reset(AggregationBuffer ab) throws HiveException {
        ((ConcatDistinctAggregationBuffer) ab).container.clear();
    }

    /**
     * Processes a new row of data into the aggregation buffer. It is on the
     * <b>MAP</b>
     * side.
     *
     * @param ab the AggregationBuffer
     * @param parameters the input
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer ab, Object[] parameters) throws HiveException {
        assert (parameters.length == 1);
        Object p = parameters[0];
        if (p != null) {
            ConcatDistinctAggregationBuffer cdab = (ConcatDistinctAggregationBuffer) ab;
            putIntoCollection(p, cdab);
        }
    }

    /**
     * Returns the contents of the current aggregation in a persistable way.
     * Here, persistable means the return value can only be built up in terms of
     * Java primitives, arrays, primitive wrappers (e.g., Double), Hadoop
     * Writables, Lists, and Maps. It is on the <b>MAP</b> side.
     *
     * @param ab the AggregationBuffer
     * @return content of the current aggregation
     * @throws HiveException
     */
    @Override
    public Object terminatePartial(AggregationBuffer ab) throws HiveException {
        ConcatDistinctAggregationBuffer cdab = (ConcatDistinctAggregationBuffer) ab;
        ArrayList<Object> ret = new ArrayList<>(cdab.container.size());
        ret.addAll(cdab.container);
        return ret;
    }

    /**
     * Merges a partial aggregation returned by terminatePartial into the
     * current aggregation. It is on the <b>REDUCE</b> side.
     *
     * @param ab the AggregationBuffer
     * @param partial the partial aggregation
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer ab, Object partial) throws HiveException {
        ConcatDistinctAggregationBuffer cdab = (ConcatDistinctAggregationBuffer) ab;
        ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
        if (partialResult != null) {
            for (Object o : partialResult) {
                putIntoCollection(o, cdab);
            }
        }
    }

    /**
     * Returns the final result of the aggregation to Hive. It is on the
     * <b>REDUCE</b> side.
     *
     * @param ab the AggregationBuffer
     * @return the final result of the aggregation
     * @throws HiveException
     */
    @Override
    public Object terminate(AggregationBuffer ab) throws HiveException {
        ConcatDistinctAggregationBuffer cdab = (ConcatDistinctAggregationBuffer) ab;
        return formatCollection(cdab.container);
    }

    /**
     * Puts the element into the collection. Will generate a nontrivial element order.
     *
     * @param p the Object we want to put int the collection
     * @param cdab the AggregationBuffer to put the elemnt into
     * @return the formatted string
     */
    private void putIntoCollection(Object p, ConcatDistinctAggregationBuffer cdab) {
        Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
        if (cdab.container.contains(pCopy)) {// to ensure the required element order
            cdab.container.remove(pCopy);
        }
        cdab.container.add(pCopy);
    }

    /**
     * Formats the collection to a string, using the value of the
     * outputDelimeter.
     *
     * @param coll the collection to be formatted
     * @return the formatted string
     */
    private static String formatCollection(Collection<Object> coll) {
        StringBuilder sb = new StringBuilder();
        for (Object s : coll) {
            sb.append(s).append(outputDelimeter);
        }
        sb.deleteCharAt(sb.length() - outputDelimeter.length());
        return sb.toString().trim();
    }
}
