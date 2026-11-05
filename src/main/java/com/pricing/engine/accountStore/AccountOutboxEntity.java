package com.pricing.engine.accountStore;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@Setter
@NoArgsConstructor
@Table(name = "batch_outbox")
public class AccountOutboxEntity {

	@Id
	@Column(name = "batch_id", nullable = false)
	private UUID batchId;

	@Column(nullable = false)
	private boolean processed;
}
