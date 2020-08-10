package com.prakash.datbase.locking.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.prakash.datbase.locking.entity.Book;

@Repository
public interface BookRepository extends JpaRepository<Book, Long>{

}
