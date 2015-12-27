import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class SmallRowSerializer implements JsonSerializer<SmallRow> {
	@Override
	  public JsonElement serialize(SmallRow smallRow, Type typeOfSrc, JsonSerializationContext context) { 
	    return context.serialize(smallRow.familyMap);
	  }
}
