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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Handles type checking for an UDAF querie.
 *
 * @author Richárd Ernő Kiss
 *
 * 
 */

//How to create the function in hive:
// hive> ADD JAR /full/path/to/udaf-concat_distinct-1.0.jar;
// hive> CREATE TEMPORARY FUNCTION concat_distinct AS 'hu.radoop.udaf.GenericUDAFConcatDistinct';

@Description(name = "concat_distinct",
        value = "_FUNC_(x) - Performs aggregation over each group. Returns a string of the objects "
                + "separated by a comma and a space. One value will only appear once.",
        extended = "Almost like SQL \"GROUP_CONCAT(DISTINCT expr)'\" or "
                + " Hive \"concat_ws( ', ', collect_set(expr))\", "
                + "although the order of the output can be different with this function.\n"
                + "Usage: SELECT att2, concat_distinct(att3) FROM table1 GROUP BY att2")

@UDFType(deterministic = true, stateful = false)

public class GenericUDAFConcatDistinct extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFConcatDistinct.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                    + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        return new GenericUDAFConcatDistinctEvaluator();
    }

}
