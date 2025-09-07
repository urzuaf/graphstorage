import com.sparsity.sparksee.gdb.*;

public class SparkseeTest {
    public static void main(String[] args) {
        try {

            // Initialize Sparksee
            SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
            Sparksee sparksee = new Sparksee(cfg);
            System.out.println("Hola");
            System.out.println("Sparksee version: "+ sparksee.getVersion());

            // Create a new database
            Database db = sparksee.create("HelloSparksee.gdb", "HelloSparksee");
            Session sess = db.newSession();
            Graph g = sess.getGraph();

            // Define schema
            int movieType = g.newNodeType("MOVIE");
            int movieTitle = g.newAttribute(movieType, "TITLE", DataType.String, AttributeKind.Basic);

            int personType = g.newNodeType("PERSON");
            int personName = g.newAttribute(personType, "NAME", DataType.String, AttributeKind.Basic);

            int directsType = g.newEdgeType("DIRECTS", true, true);
            int actsInType = g.newEdgeType("ACTS_IN", true, true);

            // Create some nodes with attributes (using Value)
            Value v = new Value();

            long woody = g.newNode(personType);
            v.setString("Woody Allen");
            g.setAttribute(woody, personName, v);

            long sofia = g.newNode(personType);
            v.setString("Sofia Coppola");
            g.setAttribute(sofia, personName, v);

            long movie1 = g.newNode(movieType);
            v.setString("Midnight in Paris");
            g.setAttribute(movie1, movieTitle, v);
            g.newEdge(directsType, woody, movie1);

            long movie2 = g.newNode(movieType);
            v.setString("Lost in Translation");
            g.setAttribute(movie2, movieTitle, v);
            g.newEdge(directsType, sofia, movie2);

            long actor = g.newNode(personType);
            v.setString("Scarlett Johansson");
            g.setAttribute(actor, personName, v);
            g.newEdge(actsInType, actor, movie2);

            // Query: actors in Sofia Coppola’s movies
            Objects sofiaMovies = g.neighbors(sofia, directsType, EdgesDirection.Outgoing);
            Objects cast = g.neighbors(sofiaMovies, actsInType, EdgesDirection.Ingoing);

            ObjectsIterator it = cast.iterator();
            while (it.hasNext()) {
                long oid = it.next();
                Value val = new Value();
                g.getAttribute(oid, personName, val);
                System.out.println("Hello " + val.getString());
            }

            // Clean up
            it.close();
            cast.close();
            sofiaMovies.close();

            sess.close();
            db.close();
            sparksee.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
