/* This file was generated by SableCC (http://www.sablecc.org/). */

package net.sf.cb2xml.sablecc.node;

import java.util.*;
import net.sf.cb2xml.sablecc.analysis.*;

public final class AValuesValueOrValues extends PValueOrValues
{
    private TValues _values_;
    private TAre _are_;

    public AValuesValueOrValues()
    {
    }

    public AValuesValueOrValues(
        TValues _values_,
        TAre _are_)
    {
        setValues(_values_);

        setAre(_are_);

    }
    public Object clone()
    {
        return new AValuesValueOrValues(
            (TValues) cloneNode(_values_),
            (TAre) cloneNode(_are_));
    }

    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAValuesValueOrValues(this);
    }

    public TValues getValues()
    {
        return _values_;
    }

    public void setValues(TValues node)
    {
        if(_values_ != null)
        {
            _values_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        _values_ = node;
    }

    public TAre getAre()
    {
        return _are_;
    }

    public void setAre(TAre node)
    {
        if(_are_ != null)
        {
            _are_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        _are_ = node;
    }

    public String toString()
    {
        return ""
            + toString(_values_)
            + toString(_are_);
    }

    void removeChild(Node child)
    {
        if(_values_ == child)
        {
            _values_ = null;
            return;
        }

        if(_are_ == child)
        {
            _are_ = null;
            return;
        }

    }

    void replaceChild(Node oldChild, Node newChild)
    {
        if(_values_ == oldChild)
        {
            setValues((TValues) newChild);
            return;
        }

        if(_are_ == oldChild)
        {
            setAre((TAre) newChild);
            return;
        }

    }
}