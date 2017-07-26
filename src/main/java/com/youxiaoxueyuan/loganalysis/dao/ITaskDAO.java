package com.youxiaoxueyuan.loganalysis.dao;

import com.youxiaoxueyuan.loganalysis.domain.Task;

public interface ITaskDAO {
	
	Task findById(long taskid);
	
}
