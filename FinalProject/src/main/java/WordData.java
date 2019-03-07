import java.util.List;

public class WordData {

    String word;
    private String type;
    int parentid;
    int id;
    WordData(String word, String type, int parentid, int id, StanfordLemmatizer lematizer) {

        List<String> stringList = lematizer.lemmatize(word);
        if (stringList.size() > 0) {
            this.word = lematizer.lemmatize(word).get(0);
        } else
            this.word = word;
        this.type=type;
        this.parentid=parentid;
        this.id=id;
    }


    boolean is_noun()
    {
        return (this.type.equals("NN")
                ||this.type.equals("NNS")
                ||this.type.equals("NNP")
                ||this.type.equals("NNPS")
        );
    }
    boolean is_verb()
    {

        return (this.type.equals("VB")
                ||this.type.equals("VBD")
                ||this.type.equals("VBG")
                ||this.type.equals("VBN")
                ||this.type.equals("VBP")
                ||this.type.equals("VBZ")
        );
    }
    public String toString()
    {
        return  this.word;
    }
}
