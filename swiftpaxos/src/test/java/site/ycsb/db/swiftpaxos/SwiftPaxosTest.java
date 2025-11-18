package site.ycsb.db.swiftpaxos;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.StringByteIterator;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

@Test
public class SwiftPaxosTest {

  SwiftPaxosClient client = new SwiftPaxosClient();

  @Test
  public void base(){
    try {
      Properties props = new Properties();
      props.setProperty(SwiftPaxosClient.MADDR_PROPERTY,"172.17.0.2");
      client.setProperties(props);
      client.init();
    } catch (DBException e) {
      e.printStackTrace();
    }

    HashMap<String, ByteIterator> map = new HashMap<>();
    map.put("k", new StringByteIterator("v"));
    client.insert("t","k", map);

    map.clear();
    client.read("t","k", Collections.EMPTY_SET, map);
    assert map.containsKey("k") & map.get("k").toString().equals("v");

    // FIXME this does not work
//    map.clear();
//    Vector<HashMap<String, ByteIterator>> vector = new Vector<>();
//    client.scan("t","k", 1, Collections.EMPTY_SET, vector);
//    System.out.println(vector);
//    assert !vector.isEmpty();
//    assert vector.get(0).get("k").toString().equals("v");

  }


}
