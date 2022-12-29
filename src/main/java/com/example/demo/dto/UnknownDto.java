package com.example.demo.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UnknownDto {

    public static UnknownDto empty() {
        return UnknownDto.builder().build();
    }

    private String value;
}
