package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import org.apache.commons.codec.digest.Md5Crypt;

import java.util.Arrays;

/**
 * Description: 对columnIndex字段做md5加密
 * User: blkfhusky
 * Date: 2019-01-24
 * Time: 10:51 AM
 */
public class Md5Transformer extends Transformer {

    public Md5Transformer() {
        setTransformerName("dx_md5");
    }

    /**
     *
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  transformer函数参数，1个参数，表示columnIndex
     * @return
     */
    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        try {
            if (paras.length != 1) {
                throw new RuntimeException("dx_md5 paras must be 1");
            }
            columnIndex = (Integer) paras[0];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        if (oriValue == null) {
            return record;
        }
        String newValue = Md5Crypt.md5Crypt(oriValue.getBytes());
        record.setColumn(columnIndex, new StringColumn(newValue));

        return record;
    }

}
