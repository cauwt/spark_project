package com.ibeifeng.sparkproject.spark;

import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * Created by zkpk on 11/13/17.
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String,String> {

    private String value = getInit();

    private String getInit(){
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0"
                ;
    }
    @Override
    public boolean isZero() {
        return this.value.equalsIgnoreCase(getInit()) ;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator other = new SessionAggrStatAccumulator();
        other.value = this.value;
        return other;
    }

    @Override
    public void reset() {
        this.value = getInit();
    }

    @Override
    public void add(String v) {
        if(StringUtils.isEmpty(this.value)){
            this.value = v;
        }
        String v1 = this.value;
        String v2 = v;
       /*
        1. find the value for key=v2 from v1
        2. add 1 to the value for key=v2
        3. replace v1 with the new value

        */
        if(StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v1)) {
            String oldValue = StringUtils.getFieldFromConcatString(v1,"\\|",v2);
            if(StringUtils.isNotEmpty(oldValue)){
                int newValue = Integer.valueOf(oldValue) +1;
                this.value = StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
            }
        }
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if(other instanceof SessionAggrStatAccumulator){
            this.value = ((SessionAggrStatAccumulator) other).value;
        } else {
            throw new UnsupportedOperationException(
                    "Cannot merge "+this.getClass().getName()+" to "+other.getClass().getName());
        }
    }

    @Override
    public String value() {
        return this.value;
    }
}
