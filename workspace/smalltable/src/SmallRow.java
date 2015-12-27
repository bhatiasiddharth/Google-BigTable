import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.annotations.Expose;


public class SmallRow{
	@Expose TreeMap<String, TreeMap<String, TreeMap<Long, String>>> familyMap 
		= new TreeMap<String, TreeMap<String, TreeMap<Long, String>>>();
	
	public SmallRow() {
		super();
	}
	
	public Set<String> getColumnFamilies() {
		return this.familyMap.keySet();
	}

	public boolean setColumn(String column, String value) {
		return setColumn(column, value, -1);
	}
	
	public boolean setColumn(String column, String value, long timestamp) {
		if(column.indexOf(":") == -1) {
			System.err.println("Invalid column family");
			return false;
		}
		String family = column.split(":")[0];
		String columnName = column.split(":")[1];
		
		if(!familyMap.containsKey(family)) {
			familyMap.put(family, new TreeMap<String, TreeMap<Long, String>>());
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		
		if(!fields.containsKey(columnName)) {
			fields.put(columnName, new TreeMap<Long, String>(Collections.reverseOrder()));
		}
		TreeMap<Long, String> field = fields.get(columnName);
		if(timestamp == -1) {
			if(field.size() > 0)
				timestamp = field.firstKey() + 1;
			else
				timestamp = 0;
		}
		field.put(timestamp, value);
		return true;
	}
	
	public String getValue(String column) {
		return getValue(column, -1);
	}
	
	public String getValue(String column, long timestamp) {
		if(column.indexOf(":") == -1) {
			System.err.println("Invalid column family");
			return null;
		}
		String family = column.split(":")[0];
		String columnName = column.split(":")[1];
		
		if(!familyMap.containsKey(family)) {
			System.err.println("Column family does not exist");
			return null;
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		
		if(!fields.containsKey(columnName)) {
			System.err.println("Column field does not exist");
			return null;
		}
		TreeMap<Long, String> field = fields.get(columnName);
		if(timestamp == -1) {
			return field.firstEntry().getValue();
		}else {
			return field.get(timestamp);
		}
	}
	
	public TreeMap<String, TreeMap<Long, String>> getFamily(String family) {
		if(family.indexOf(":") != -1) {
			family = family.split(":")[0];
		}
		
		if(!familyMap.containsKey(family)) {
			System.err.println("Column family does not exist");
			return null;
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		
		return fields;
	}
	
	public TreeMap<Long, String> getColumn(String column) {
		if(column.indexOf(":") == -1) {
			System.err.println("Invalid column family");
			return null;
		}
		String family = column.split(":")[0];
		String columnName = column.split(":")[1];
		
		if(!familyMap.containsKey(family)) {
			System.err.println("Column family does not exist");
			return null;
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		
		return fields.get(columnName);
	}
	
	public boolean deleteColumn(String column) {
		if(column.indexOf(":") == -1) {
			System.err.println("Invalid column family");
			return false;
		}
		String family = column.split(":")[0];
		String columnName = column.split(":")[1];
		
		if(!familyMap.containsKey(family)) {
			System.err.println("Column family does not exist");
			return false;
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		if(fields.containsKey(columnName)) {
			fields.remove(columnName);
			return true;
		}
		return false;
	}
	
	public boolean deletefamily(String family) {
		if(family.indexOf(":") != -1) {
			family = family.split(":")[0];
		}
		
		if(familyMap.containsKey(family)) {
			familyMap.remove(family);
			return true;
		}
		
		return false;
	}
	
	public boolean deleteValue(String column) {
		return deleteValue(column, -1);
	}
	public boolean deleteValue(String column, long timestamp) {
		if(column.indexOf(":") == -1) {
			System.err.println("Invalid column family");
			return false;
		}
		String family = column.split(":")[0];
		String columnName = column.split(":")[1];
		
		if(!familyMap.containsKey(family)) {
			System.err.println("Column family does not exist");
			return false;
		}
		TreeMap<String, TreeMap<Long, String>> fields = familyMap.get(family);
		if(!fields.containsKey(columnName)) {
			System.err.println("Column field does not exist");
			return false;
		}
		
		TreeMap<Long, String> field = fields.get(columnName);
		if(timestamp == -1) {
			field.remove(field.firstEntry());
			return true;
		}
		
		if(field.containsKey(timestamp)) {
			field.remove(field.get(timestamp));
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return Utils.gson.toJson(this);
	}

	
}
