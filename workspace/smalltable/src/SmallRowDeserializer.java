import java.lang.reflect.Type;
import java.util.TreeMap;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;


public class SmallRowDeserializer implements JsonDeserializer<SmallRow> {
	@Override
    public SmallRow deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException  {
        SmallRow smallRow = new SmallRow();
    	smallRow.familyMap
    	= context.deserialize(json, new TypeToken<TreeMap<String, TreeMap<String, TreeMap<Long, String>>>>(){}.getType());
    	
        return smallRow;
    }
}
