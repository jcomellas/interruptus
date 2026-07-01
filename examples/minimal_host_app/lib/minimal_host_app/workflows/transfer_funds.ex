defmodule MinimalHostApp.Workflows.TransferFunds do
  @moduledoc """
  Example durable workflow: debit then credit with checkpoint verification.
  """

  use Interruptus.Workflow

  workflow do
    param :from_account_id
    param :to_account_id
    param :amount

    data :debit_ref
    data :credit_ref

    pipeline :validate_accounts

    checkpoint do
      verify :verify_debit_applied
      pipeline :debit_account
    end

    checkpoint do
      verify :verify_credit_applied
      pipeline :credit_account
    end

    pipeline :send_receipt

    restart_policy max_attempts: 3, backoff: :exponential
    rollback_policy compensate: [:reverse_debit, :reverse_credit]
  end

  def validate_accounts(command, params, _data) do
    if params.amount > 0 do
      command
    else
      command |> Interruptus.Command.put_error(:amount, :invalid) |> Interruptus.Command.halt()
    end
  end

  def debit_account(command, _params, _data) do
    ref = "debit-#{command.params.from_account_id}-#{command.params.amount}"
    Interruptus.Command.put_data(command, :debit_ref, ref)
  end

  def credit_account(command, _params, _data) do
    ref = "credit-#{command.params.to_account_id}-#{command.params.amount}"
    Interruptus.Command.put_data(command, :credit_ref, ref)
  end

  def send_receipt(command, _params, _data), do: command

  def verify_debit_applied(%{data: %{debit_ref: ref}}) when is_binary(ref), do: :done
  def verify_debit_applied(_), do: :not_done

  def verify_credit_applied(%{data: %{credit_ref: ref}}) when is_binary(ref), do: :done
  def verify_credit_applied(_), do: :not_done

  def reverse_debit(command), do: command
  def reverse_credit(command), do: command
end
