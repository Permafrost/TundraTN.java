/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023 Lachlan Dowding
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package permafrost.tundra.tn.document;

import com.wm.app.tn.doc.BizDocAttribute;
import com.wm.app.tn.doc.BizDocAttributeTransform;
import com.wm.data.IData;
import java.sql.Timestamp;

/**
 * A BizDocAttributeTransform that proxies method calls to another BizDocAttributeTransform object.
 */
abstract public class ProxyBizDocAttributeTransform extends BizDocAttributeTransform {
    /**
     * The object method calls are proxied to.
     */
    protected BizDocAttributeTransform transformer;

    /**
     * Construct a new ProxyBizDocAttributeTransform object.
     *
     * @param transformer   The BizDocAttributeTransform object method calls will be proxied to.
     */
    ProxyBizDocAttributeTransform(BizDocAttributeTransform transformer) {
        this.transformer = transformer;
    }

    @Override
    public BizDocAttribute getAttribute() {
        return transformer.getAttribute();
    }

    @Override
    public Object apply(String[] values) throws Exception {
        return transformer.apply(values);
    }

    @Override
    public Object applyCustom(String[] values, IData data) throws Exception {
        return transformer.applyCustom(values, data);
    }

    @Override
    public void setIDTypeName(String idTypeName) {
        transformer.setIDTypeName(idTypeName);
    }

    @Override
    public String getIDTypeName() {
        return transformer.getIDTypeName();
    }

    @Override
    public Timestamp applyDate(String[] values) {
        return transformer.applyDate(values);
    }

    public Timestamp[] applyDateList(String[] values) {
        return transformer.applyDateList(values);
    }

    public String[] getPreFnArgs() {
        return transformer.getPreFnArgs();
    }

    @Override
    public String getPreFnArg() {
        return transformer.getPreFnArg();
    }

    @Override
    public int getPreFunction() {
        return transformer.getPreFunction();
    }

    @Override
    public String[] getArgs() {
        return transformer.getArgs();
    }

    @Override
    public String getArg() {
        return transformer.getArg();
    }

    @Override
    public String getCustomSvc() {
        return transformer.getCustomSvc();
    }

    @Override
    public int getFunction() {
        return transformer.getFunction();
    }

    @Override
    public String getFunctionName() {
        return transformer.getFunctionName();
    }

    @Override
    public String getPreFunctionName() {
        return transformer.getPreFunctionName();
    }

    @Override
    public void setPreFnArgs(String[] args) {
        transformer.setPreFnArgs(args);
    }

    @Override
    public void setPreFnArg(String arg) {
        transformer.setPreFnArg(arg);
    }

    @Override
    public void setPreFunction(int fn) {
        transformer.setPreFunction(fn);
    }

    @Override
    public void setArgs(String[] args) {
        transformer.setArgs(args);
    }

    @Override
    public void setArg(String arg) {
        transformer.setArg(arg);
    }

    @Override
    public void setCustomSvc(String svc) {
        transformer.setCustomSvc(svc);
    }

    @Override
    public void setFunction(int fn) {
        transformer.setFunction(fn);
    }

    @Override
    public void setAttribute(BizDocAttribute att) {
        transformer.setAttribute(att);
    }

    @Override
    public String toString() {
        return transformer.toString();
    }

    @Override
    public IData getIData() {
        return transformer.getIData();
    }

    @Override
    public void setIData(IData idata) {
        transformer.setIData(idata);
    }
}
