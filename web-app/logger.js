var request = require('request')
    ;

var logger = module.exports;

var loggerRESTAPIURL = "http://129.150.91.133/SoaringTheWorldAtRestService/resources/logger/log";
                        
var apiURL = "/logger-api";

logger.DEBUG = "debug";
logger.INFO = "info";
logger.WARN = "warning";
logger.ERROR = "error";

logger.log =
    function (message, moduleName, loglevel) {

        /* POST:
      
  {
      "logLevel" : "info"
      ,"module" : "soaring.clouds.accs.artist-api"
      , "message" : "starting a new logger module - message from ACCS"
  	
  }
  */
        var logRecord = {
            "logLevel": loglevel
            , "module": "soaring.clouds." + moduleName
            , "message": message

        };
        var args = {
            data: JSON.stringify(logRecord),
            headers: { "Content-Type": "application/json" }
        };

        var route_options = {};


            var msg = {
                "records": [{
                    "key": "log", "value": {
                        "logLevel": loglevel
                        , "module": "soaring.clouds." + moduleName
                        , "message": message
                        , "timestamp": Date.now()
                        , "eventType": "log"

                    }
                }]
            };

// Issue the POST  -- the callback will return the response to the user
        route_options.method = "POST";
        //            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(keyString);
        route_options.uri = loggerRESTAPIURL;
        console.log("Logger Target URL " + route_options.uri);

        route_options.body = args.data;
        route_options.headers = args.headers;

        request(route_options, function (error, rawResponse, body) {
            if (error) {
                console.log(JSON.stringify(error));
            } else {
                console.log(rawResponse.statusCode);
                console.log("BODY:" + JSON.stringify(body));
            }//else

        });//request

    }//logger.log
console.log("Logger API initialized at " + apiURL + " running against Logger Service URL " + loggerRESTAPIURL);
