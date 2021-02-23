package ru.miro.hr.task.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.GATEWAY_TIMEOUT)
public class TimeoutRuntimeException extends RuntimeException {

    public TimeoutRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TimeoutRuntimeException(Throwable cause) {
        super(cause);
    }
}
