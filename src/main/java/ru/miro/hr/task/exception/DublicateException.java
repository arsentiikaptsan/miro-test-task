package ru.miro.hr.task.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class DublicateException extends RuntimeException {
    public DublicateException() {
    }

    public DublicateException(String message) {
        super(message);
    }
}
