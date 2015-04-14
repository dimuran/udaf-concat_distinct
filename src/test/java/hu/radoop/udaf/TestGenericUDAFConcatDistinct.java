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

/**
 *
 * @author Richárd Ernő Kiss
 */
package hu.radoop.udaf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class TestGenericUDAFConcatDistinct extends TestCase {

    public void testCorr() throws HiveException {
        
        GenericUDAFConcatDistinct ConcatDistinct = new GenericUDAFConcatDistinct();
        GenericUDAFEvaluator eval1 = ConcatDistinct.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.stringTypeInfo});
        GenericUDAFEvaluator eval2 = ConcatDistinct.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.stringTypeInfo});

        ObjectInspector poi1 = eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector});
        ObjectInspector poi2 = eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector});

        //Map1
        GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
        eval1.iterate(buffer1, new Object[]{"apple"});
        eval1.iterate(buffer1, new Object[]{"peach"});
        Object mapOutput1 = eval1.terminatePartial(buffer1);

        //Map2
        GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
        eval1.iterate(buffer2, new Object[]{"apple"});
        Object mapOutput2 = eval2.terminatePartial(buffer2);

        ObjectInspector coi = eval2.init(GenericUDAFEvaluator.Mode.FINAL,
                new ObjectInspector[]{poi1});
        //Reduce
        GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
        eval2.merge(buffer3, mapOutput1);
        eval2.merge(buffer3, mapOutput2);

        Object result = eval2.terminate(buffer3);
        assertEquals("peach, apple", String.valueOf(result));
    }
}