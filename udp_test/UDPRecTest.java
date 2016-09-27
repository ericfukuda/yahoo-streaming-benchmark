import java.io.*;//BufferedReadered用
import java.net.*;//DatagramSocket用

public class UDPRecTest{
  static BufferedReader br = new BufferedReader( new InputStreamReader(System.in) );

  DatagramSocket recSocket = new DatagramSocket(5431);//UDP受信用ソケット
  SocketAddress sockAddress;//接続してきた送信元

  public UDPRecTest() throws Exception {//コンストラクタ
  }

  public boolean receive() throws Exception {
    byte []buf = new byte[512];
    DatagramPacket packet= new DatagramPacket(buf,buf.length);
    recSocket.receive(packet);//受信 & wait
    sockAddress = packet.getSocketAddress(); //送信元情報取得

    int len = packet.getLength();//受信バイト数取得
    String msg = new String(buf, 0, len);
    //if(msg.equals("")){
    //  return false;//入力文字列なしで終了
    //}
    //System.out.println(msg + ":" + len + "byte receive by "+ sockAddress.toString());
    //System.out.println(msg);
    return true;
  }
  public static void main(String [] args) throws Exception{
    UDPRecTest test = new UDPRecTest();

    while(test.receive() == true)//受信の繰り返し
      ;

    System.out.print("Press Enter Key");
    br.readLine();
  }
}
