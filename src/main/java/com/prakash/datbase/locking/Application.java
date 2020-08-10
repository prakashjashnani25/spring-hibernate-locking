package com.prakash.datbase.locking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import com.prakash.datbase.locking.repository.BookRepository;

@SpringBootApplication
@EnableJpaRepositories(basePackages= {"com.prakash.datbase.locking"})
@EntityScan("com.prakash.datbase.locking")
public class Application {

	public static void main(String ...args) {
		ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
		System.out.println(context.getBean(BookRepository.class).findAll());
	}
	
}
