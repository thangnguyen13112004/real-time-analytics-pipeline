package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class SalesPerCategory {
    private Date transactionDate;
    private String category;
    private double totalSales;
}
