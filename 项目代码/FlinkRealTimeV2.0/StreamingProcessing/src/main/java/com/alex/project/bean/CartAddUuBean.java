package com.alex.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;

    public String getStt() {
        return stt;
    }

    public void setStt(String stt) {
        this.stt = stt;
    }

    public String getEdt() {
        return edt;
    }

    public void setEdt(String edt) {
        this.edt = edt;
    }

    public Long getCartAddUuCt() {
        return cartAddUuCt;
    }

    public void setCartAddUuCt(Long cartAddUuCt) {
        this.cartAddUuCt = cartAddUuCt;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public CartAddUuBean(String stt, String edt, Long cartAddUuCt, Long ts) {
        this.stt = stt;
        this.edt = edt;
        this.cartAddUuCt = cartAddUuCt;
        this.ts = ts;
    }
}

