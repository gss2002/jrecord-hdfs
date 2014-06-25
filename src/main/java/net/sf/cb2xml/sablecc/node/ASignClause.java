/* This file was generated by SableCC (http://www.sablecc.org/). */

package net.sf.cb2xml.sablecc.node;

import java.util.*;
import net.sf.cb2xml.sablecc.analysis.*;

public final class ASignClause extends PSignClause
{
    private PSignIs _signIs_;
    private PLeadingOrTrailing _leadingOrTrailing_;
    private PSeparateCharacter _separateCharacter_;

    public ASignClause()
    {
    }

    public ASignClause(
        PSignIs _signIs_,
        PLeadingOrTrailing _leadingOrTrailing_,
        PSeparateCharacter _separateCharacter_)
    {
        setSignIs(_signIs_);

        setLeadingOrTrailing(_leadingOrTrailing_);

        setSeparateCharacter(_separateCharacter_);

    }
    public Object clone()
    {
        return new ASignClause(
            (PSignIs) cloneNode(_signIs_),
            (PLeadingOrTrailing) cloneNode(_leadingOrTrailing_),
            (PSeparateCharacter) cloneNode(_separateCharacter_));
    }

    public void apply(Switch sw)
    {
        ((Analysis) sw).caseASignClause(this);
    }

    public PSignIs getSignIs()
    {
        return _signIs_;
    }

    public void setSignIs(PSignIs node)
    {
        if(_signIs_ != null)
        {
            _signIs_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        _signIs_ = node;
    }

    public PLeadingOrTrailing getLeadingOrTrailing()
    {
        return _leadingOrTrailing_;
    }

    public void setLeadingOrTrailing(PLeadingOrTrailing node)
    {
        if(_leadingOrTrailing_ != null)
        {
            _leadingOrTrailing_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        _leadingOrTrailing_ = node;
    }

    public PSeparateCharacter getSeparateCharacter()
    {
        return _separateCharacter_;
    }

    public void setSeparateCharacter(PSeparateCharacter node)
    {
        if(_separateCharacter_ != null)
        {
            _separateCharacter_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        _separateCharacter_ = node;
    }

    public String toString()
    {
        return ""
            + toString(_signIs_)
            + toString(_leadingOrTrailing_)
            + toString(_separateCharacter_);
    }

    void removeChild(Node child)
    {
        if(_signIs_ == child)
        {
            _signIs_ = null;
            return;
        }

        if(_leadingOrTrailing_ == child)
        {
            _leadingOrTrailing_ = null;
            return;
        }

        if(_separateCharacter_ == child)
        {
            _separateCharacter_ = null;
            return;
        }

    }

    void replaceChild(Node oldChild, Node newChild)
    {
        if(_signIs_ == oldChild)
        {
            setSignIs((PSignIs) newChild);
            return;
        }

        if(_leadingOrTrailing_ == oldChild)
        {
            setLeadingOrTrailing((PLeadingOrTrailing) newChild);
            return;
        }

        if(_separateCharacter_ == oldChild)
        {
            setSeparateCharacter((PSeparateCharacter) newChild);
            return;
        }

    }
}
