package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class SalesPerDay {
    private Date transactionDate;
    private Double totalSales ;
}
