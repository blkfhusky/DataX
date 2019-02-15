package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * 首尾保留，替换中间字符
 *
 * @author zhuxucheng
 * @date 2019-02-13 5:51 PM
 */
public class CenterReplaceTransformer extends Transformer {

    public CenterReplaceTransformer() {
        setTransformerName("dx_centerrep");
    }

    /**
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  transformer函数参数，5个参数 [columnIndex, 首部保留长度, 尾部保留长度, 中间替换字符, 替换长度]
     * @return
     */
    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        int frontLen;
        int tailLen;
        char replaceChar;
        int replaceLen;
        //真实的替换长度，如果为-1表示替换长度为 原值长度-frontLen-tailLen
        int realReplaceLen;
        try {
            if (paras.length != 5) {
                throw new RuntimeException("dx_centerrep paras must be 5");
            }
            columnIndex = (Integer) paras[0];
            frontLen = Integer.valueOf((String) paras[1]);
            tailLen = Integer.valueOf((String) paras[2]);
            String replaceStr = (String) paras[3];
            replaceLen = Integer.valueOf((String) paras[4]);
            if (replaceStr.length() != 1) {
                throw new RuntimeException(String.format("dx_pad forth para(%s) must be a char", replaceStr));
            }
            replaceChar = replaceStr.charAt(0);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (oriValue == null) {
                oriValue = "";
            }
            realReplaceLen = (replaceLen == -1) ? oriValue.length() - frontLen - tailLen : replaceLen;
            String newValue;
            if (oriValue.length() > (frontLen + tailLen)) {
                newValue = doReplace(oriValue, frontLen, tailLen, replaceChar, realReplaceLen);
            } else if (oriValue.length() > frontLen) {
                newValue = doReplace(oriValue, frontLen, 0, replaceChar, oriValue.length() - frontLen);
            } else {
                newValue = doReplace(oriValue, 0, 0, replaceChar, oriValue.length());
            }
            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }

        return record;
    }

    private String doReplace(String oriValue, int frontLen, int tailLen, char replaceChar, int replaceLen) {
        StringBuilder replaceStr = new StringBuilder();
        for (int i = 0; i < replaceLen; i++) {
            replaceStr.append(replaceChar);
        }
        replaceStr.insert(0, oriValue.substring(0, frontLen));
        replaceStr.append(oriValue, oriValue.length() - tailLen, oriValue.length());
        return replaceStr.toString();
    }
}
