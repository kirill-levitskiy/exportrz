package com.export;

import java.sql.ResultSet;
import org.json.JSONArray;
import org.json.JSONObject;

class JsonConverter {

  static JSONArray convertToJSON(ResultSet resultSet) throws Exception {
    JSONArray jsonArray = new JSONArray();
    while (resultSet.next()) {
      JSONObject obj = null;
      obj = new JSONObject();
      int totalRows = resultSet.getMetaData().getColumnCount();
      for (int i = 0; i < totalRows; i++) {
        obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(),
            resultSet.getObject(i + 1) != null
            ? resultSet.getObject(i + 1)
            : ""
        );
      }
      jsonArray.put(obj);     }

    return jsonArray;
  }

}
