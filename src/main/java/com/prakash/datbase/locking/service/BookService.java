package com.prakash.datbase.locking.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

import com.prakash.datbase.locking.entity.Book;
import com.prakash.datbase.locking.repository.BookRepository;

@Service
public class BookService {

	@Autowired
	private BookRepository bookRepository;
	
	public  List<Book> getAllBooks(){
		return bookRepository.findAll();
	}
}
