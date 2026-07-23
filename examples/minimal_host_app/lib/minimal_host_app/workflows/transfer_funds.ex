defmodule MinimalHostApp.Workflows.TransferFunds do
  @moduledoc """
  Example durable workflow: debit then credit with checkpoint verification.

  Demonstrates `Interruptus.Effect` for at-least-once-safe stage markers and
  per-checkpoint compensations: on rollback, only checkpoints the workflow
  actually passed are compensated, in LIFO order (credit reversed before
  debit). Real money movement should still use domain unique constraints;
  markers make successful completion skippable on replay and usable from
  `verify/1`.
  """

  use Interruptus.Workflow

  workflow do
    param :from_account_id, :integer
    param :to_account_id, :integer
    param :amount, :decimal

    data :debit_ref, :string
    data :credit_ref, :string

    stage_timeout 30_000

    pipeline :validate_accounts

    checkpoint compensate: :reverse_debit do
      verify :verify_debit_applied
      pipeline :debit_account
    end

    checkpoint compensate: :reverse_credit do
      verify :verify_credit_applied
      pipeline :credit_account
    end

    pipeline :send_receipt

    restart_policy max_attempts: 3, backoff: :exponential
  end

  def validate_accounts(command, params, _data) do
    if Decimal.compare(params.amount, Decimal.new(0)) == :gt do
      command
    else
      command |> Interruptus.Command.put_error(:amount, :invalid) |> Interruptus.Command.halt()
    end
  end

  def debit_account(command, params, _data) do
    key = "debit:#{params.from_account_id}:#{params.amount}"

    Interruptus.Effect.once(command, key, fn cmd ->
      ref = "debit-#{params.from_account_id}-#{params.amount}"
      Interruptus.Command.put_data(cmd, :debit_ref, ref)
    end)
  end

  def credit_account(command, params, _data) do
    key = "credit:#{params.to_account_id}:#{params.amount}"

    Interruptus.Effect.once(command, key, fn cmd ->
      ref = "credit-#{params.to_account_id}-#{params.amount}"
      Interruptus.Command.put_data(cmd, :credit_ref, ref)
    end)
  end

  def send_receipt(command, _params, _data), do: command

  def verify_debit_applied(command) do
    params = command.params
    key = "debit:#{params.from_account_id}:#{params.amount}"

    cond do
      Interruptus.Effect.exists?(command, key) -> :done
      is_binary(command.data.debit_ref) -> :done
      true -> :not_done
    end
  end

  def verify_credit_applied(command) do
    params = command.params
    key = "credit:#{params.to_account_id}:#{params.amount}"

    cond do
      Interruptus.Effect.exists?(command, key) -> :done
      is_binary(command.data.credit_ref) -> :done
      true -> :not_done
    end
  end

  # Compensations must be idempotent: the step in flight during a crash runs
  # again after reclaim (compensation progress is otherwise persisted per step).
  def reverse_debit(command), do: command
  def reverse_credit(command), do: command
end
