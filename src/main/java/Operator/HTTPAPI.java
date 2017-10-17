package Operator;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.*;
import java.io.*;
import java.util.*;

public class HTTPAPI {

    public static void main(String[] args) {

       JSONObject statusJSONObj = constructStatusJSON();
       ServerResponse serverResponseObj = statusHTTPPOST(statusJSONObj,"http://127.0.0.1:5050/api/v1");
       parseGetHealthResponse(serverResponseObj.responseString);

       JSONArray reserveJSONObj = constructReservationJSON("r1",1.0,2048.0);
       reservationHTTPPost(reserveJSONObj,"584c6500-6a5f-4a9e-8ac7-ddda16fe31a5-S0","http://127.0.0.1:5050/master/reserve");
    }

    private static ServerResponse reservationHTTPPost(JSONArray obj, String agentId, String httpURI)
    {
        String outputStr = "";
        ServerResponse serverResponseObj = new ServerResponse();
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost post = new HttpPost(httpURI);
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("slaveId", agentId));
            params.add(new BasicNameValuePair("resources", obj.toString()));
            CloseableHttpResponse response = null;
            try {
                post.setEntity(new UrlEncodedFormEntity(params));
                post.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
                post.setHeader("Accept", "application/json");
                response = httpClient.execute(post);
                System.out.println(response.getStatusLine());
                serverResponseObj.statusCode=response.getStatusLine().getStatusCode();
                BufferedReader br = new BufferedReader(
                        new InputStreamReader((response.getEntity().getContent())));

                String tmpStr;
                while ((tmpStr = br.readLine()) != null) {
                    outputStr += tmpStr;
                }
            } finally {
                response.close();
            }

        }catch(Exception e)
        {
            e.printStackTrace();
        }
        serverResponseObj.responseString=outputStr;
        return serverResponseObj;
    }
    private static ServerResponse statusHTTPPOST(JSONObject obj, String httpURI) {

        String outputStr = "";
        ServerResponse serverResponseObj = new ServerResponse();
        try {

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost postRequest = new HttpPost(httpURI);

            StringEntity input = new StringEntity(obj.toString());

            postRequest.setEntity(input);
            postRequest.setHeader("Content-Type", "application/json");
            postRequest.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpClient.execute(postRequest);

            System.out.println(response.getStatusLine());
            serverResponseObj.statusCode=response.getStatusLine().getStatusCode();

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String tmpStr;
            while ((tmpStr = br.readLine()) != null) {
                outputStr += tmpStr;
            }

            httpClient.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

        serverResponseObj.responseString=outputStr;
        return serverResponseObj;
    }

    private static JSONArray constructReservationJSON(String role, Double cpu, Double mem) {
        JSONArray resourceArray = new JSONArray();
        JSONObject cpuObj = new JSONObject();
        JSONObject memObj = new JSONObject();
        try {

            cpuObj.put("type","SCALAR");
            cpuObj.put("name","cpus");
            cpuObj.put("reservation",new JSONObject());
            cpuObj.put("role",role);
            cpuObj.put("scalar",new JSONObject().put("value",cpu));

            memObj.put("type","SCALAR");
            memObj.put("name","mem");
            memObj.put("reservation",new JSONObject());
            memObj.put("role",role);
            memObj.put("scalar",new JSONObject().put("value",mem));

            resourceArray.put(cpuObj);
            resourceArray.put(memObj);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resourceArray;
    }
    private static JSONObject constructStatusJSON() {
        JSONObject obj = new JSONObject();
        try {
            obj.put("type", "GET_HEALTH");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj;
    }
    private static void parseGetHealthResponse(String responseStr){
        Boolean health = null;
        try {
            health = (Boolean) new JSONObject(responseStr).getJSONObject("get_health").get("healthy");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(health);
    }
}