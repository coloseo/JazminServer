/*
 * jazmin boot script template.variable '$' or 'jazmin' support function to control
 * jazmin server. 
 */
$.setLogLevel('ALL');
$.setLogFile('./log/'+$.getServerName()+".log",true);
//$.disableConsoleLog();
//
var defaultProtobufInvokeService = new DefaultProtobufInvokeService();
var protobufServer = new ProtobufServer();
protobufServer.setProtobufInvokeService(defaultProtobufInvokeService);
$.addServer(protobufServer);
$.addServer(new RpcServer());
$.addServer(new ConsoleServer());
$.addServer(new WebServer());