package com.youxiaoxueyuan.loganalysis.util;


public class StringUtils {


	public static boolean isNotEmpty(String str) {
		return str != null && !"".equals(str);
	}
	
	public static String trimComma(String str) {
		if(str.startsWith(",")) {
			str = str.substring(1);
		}
		if(str.endsWith(",")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	
	public static String fulfuill(String str) {
		if(str.length() == 2) {
			return str;
		} else {
			return "0" + str;
		}
	}
	
	public static String getFieldFromConcatString(String str,
			String delimiter, String field) {
		try {
			String[] fields = str.split(delimiter);
			for(String concatField : fields) {
				if(concatField.split("=").length == 2) {
					String fieldName = concatField.split("=")[0];
					String fieldValue = concatField.split("=")[1];
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
