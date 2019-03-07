public class Triple_SW extends  Triple {

    Triple_SW()
    {

    }
    Triple_SW(String p,String s, String w)
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



        if(z!=0)
        {

            if(this.w.equals("*")&&y==0&&x==0)
            {
                return -1;
            }
            if(o.w.equals("*")&&y==0&&x==0)
            {
                return 1;
            }
            return z;
        }
        if(y!=0)
        {
            return y;
        }
        if(x!=0)
        {
            return x;
        }

        return 0;
    }
}
