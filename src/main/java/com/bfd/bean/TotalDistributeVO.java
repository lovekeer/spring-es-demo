package com.bfd.bean;

import lombok.Data;

/**
 * 调用总次数统计
 *
 * @Description:
 * @Date:
 * @Modified by:
 */
@Data
public class TotalDistributeVO {

    private int num;  // 序列号

    private String time;  // 时间

    private long total;  // 调用总次数

    private long successTotal;  // 调用成功次数

    private long failTotal;  // 调用失败次数

}
