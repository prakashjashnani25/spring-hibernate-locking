package com.prakash.datbase.locking.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.prakash.datbase.locking.service.BookService;

@SpringBootTest
@RunWith(SpringRunner.class)
public class BookServiceUnitTest {

	@Autowired
	private BookService boookService;
	
	@Test
	public void  whenApplicationStart_thenHibernateCreateInitialRecords() {
		boookService.getAllBooks();
	}
}
