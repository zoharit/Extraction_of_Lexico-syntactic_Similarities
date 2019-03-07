public class Triple_PS extends Triple {
    Triple_PS()
    {
    }
    Triple_PS(String p,String s, String w)
    {
        this.p=p;
        this.s=s;
        this.w=w;
    }

    public int compareTo(Triple o)
    {
        int  x = this.p.compareTo(o.p);
        int  y = this.s.compareTo(o.s);
        int z = this.w.compareTo(o.w);
        if(x!=0)
        {
            if(o.p.equals("*")&&y==0&&z==0)
            {
                return 1;
            }
            if(this.p.equals("*")&&y==0&&z==0)
            {
                return -1;
            }
            return x;
        }
        if(y!=0)
        {
            return y;
        }
        if(z!=0)
        {
            return z;
        }
        return 0;
    }
}
