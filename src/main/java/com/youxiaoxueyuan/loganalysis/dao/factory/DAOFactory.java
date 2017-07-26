package com.youxiaoxueyuan.loganalysis.dao.factory;

import com.youxiaoxueyuan.loganalysis.dao.ITaskDAO;
import com.youxiaoxueyuan.loganalysis.dao.impl.TaskDAOImpl;

public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl ();
	}

}
