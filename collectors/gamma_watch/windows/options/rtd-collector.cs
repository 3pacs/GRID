using System;
using System.IO;
using System.Threading;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using Microsoft.Office.Interop.Excel;

[ComVisible(true),ClassInterface(ClassInterfaceType.None)]
public class GexCallback : IRTDUpdateEvent {
 public volatile int Notifications=0;
 public volatile bool Disconnected=false;
 public void UpdateNotify(){Notifications++;}
 public int HeartbeatInterval{get;set;}
 public void Disconnect(){Disconnected=true;}
}
public class GexCollector {
 public static void Run(string root){
  var json=new System.Web.Script.Serialization.JavaScriptSerializer();json.MaxJsonLength=8000000;
  var topics=json.Deserialize<List<Dictionary<string,object>>>(File.ReadAllText(Path.Combine(root,"topics.json")));
  var srv=(IRtdServer)Activator.CreateInstance(Type.GetTypeFromProgID("tos.rtd",true));
  var cb=new GexCallback();cb.HeartbeatInterval=2000;
  int started=srv.ServerStart(cb);if(started<=0)throw new Exception("RTD start failed");
  var records=new Dictionary<int,Dictionary<string,object>>();int count=0;long updates=0;
  DateTime end=DateTime.UtcNow.Date.AddHours(20).AddMinutes(20),lastWrite=DateTime.MinValue,lastHeart=DateTime.MinValue;int heartbeat=0;
  if(end<=DateTime.UtcNow)end=DateTime.UtcNow.AddMinutes(1);
  try{
   for(int i=0;i<topics.Count;i++){
    string field=(string)topics[i]["field"],symbol=(string)topics[i]["symbol"];
    if(Array.IndexOf(new string[]{"LAST","BID","ASK","GAMMA","IMPL_VOL","OPEN_INT"},field)<0)throw new Exception("Field not permitted");
    if(symbol!="SPY" && !System.Text.RegularExpressions.Regex.IsMatch(symbol,@"^\.SPY\d{6}[CP]\d+(\.\d+)?$"))throw new Exception("Symbol not permitted");
    Array args=new object[]{field,symbol};bool fresh=true;object v=srv.ConnectData(i+1,ref args,ref fresh);count=i+1;
    records[count]=new Dictionary<string,object>{{"symbol",symbol},{"field",field},{"value",v},{"received_at",DateTime.UtcNow.ToString("o")},{"callback_seen",false}};
    if(i%25==0)System.Windows.Forms.Application.DoEvents();
   }
   while(DateTime.UtcNow<end && !cb.Disconnected){
    System.Windows.Forms.Application.DoEvents();Thread.Sleep(50);
    if(cb.Notifications>0){cb.Notifications=0;int n=0;Array a=srv.RefreshData(ref n);for(int i=0;i<n;i++){int id=Convert.ToInt32(a.GetValue(0,i));if(records.ContainsKey(id)){records[id]["value"]=a.GetValue(1,i);records[id]["received_at"]=DateTime.UtcNow.ToString("o");records[id]["callback_seen"]=true;updates++;}}}
    if((DateTime.UtcNow-lastHeart).TotalSeconds>=10){heartbeat=srv.Heartbeat();lastHeart=DateTime.UtcNow;if(heartbeat<=0)throw new Exception("RTD heartbeat failed");}
    if((DateTime.UtcNow-lastWrite).TotalSeconds>=2){
     var snapshot=new{schema="spy-rtd-v1",host=Environment.MachineName,source="thinkorswim RTD",collected_at=DateTime.UtcNow.ToString("o"),heartbeat=heartbeat,update_count=updates,topic_count=count,exchange_timestamp_available=false,entitlement="not_verified",records=new List<Dictionary<string,object>>(records.Values)};
     string path=Path.Combine(root,"snapshot.json"),tmp=Path.Combine(root,"snapshot.tmp.json");try{File.WriteAllText(tmp,json.Serialize(snapshot));if(File.Exists(path))File.Replace(tmp,path,null);else File.Move(tmp,path);}catch(IOException){}lastWrite=DateTime.UtcNow;
    }
   }
  }finally{for(int i=1;i<=count;i++){try{srv.DisconnectData(i);}catch{}}try{srv.ServerTerminate();}catch{}Marshal.ReleaseComObject(srv);}
 }
}
