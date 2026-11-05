package com.pricing.engine.accountStore;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@Setter
@NoArgsConstructor
@Table(name = "accounts")
public class AccountEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "batch_id", nullable = false)
	private AccountOutboxEntity batch;

	@Column(name = "account_number", nullable = false, length = 25)
	private String accountNumber;

	@Column(name = "product_code", nullable = false, length = 25)
	private String productCode;

	@Column(name = "branch_code", nullable = false, length = 25)
	private String branchCode;

	@Column(name = "pricing_date", nullable = false)
	private LocalDate pricingDate;

	@Default
	@OneToMany(mappedBy = "account")
	private List<AccountAttributeEntity> attributes = new ArrayList<>();

	@Default
	@OneToMany(mappedBy = "account")
	private List<FeeRequestEntity> fees = new ArrayList<>();
}
