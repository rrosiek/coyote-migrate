<?php

namespace App\Console\Commands;

use Carbon\Carbon;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class CoyoteMigrate extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'coyote:migrate';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Migrate Coyote MySQL to PostgreSQL';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        try {
            $this->checkTablesContent();
            $this->addRoles();
            $this->migrateUsers();
            $this->migrateEmailErrors();
            $this->migrateBrotherTree();
            $this->migrateUploads();
            $this->migrateCorrespondence();
            $this->migratePayments();
        } catch (Exception $e) {
            $this->error($e->getMessage());

            return;
        }
    }

    protected function addRoles()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('roles')->insert([
                ['id' => 1, 'level' => 0, 'name' => 'Guest'],
                ['id' => 2, 'level' => 1, 'name' => 'Member'],
                ['id' => 3, 'level' => 10, 'name' => 'Administrator'],
            ]);
        });
    }

    protected function checkTablesContent()
    {
        $tables = [
            'correspondence',
            'files',
            'minutes',
            'newsletters',
            'uploads',
            'brother_relations',
            'email_failures',
            'roles',
            'users'
        ];

        $rows = collect($tables)->map(function ($table) {
            return DB::connection(env('PGSQL_CONNECTION'))->table($table)->count();
        })->sum();

        if ($rows > 0) {
            if ($this->confirm('Migration database contains data, would you like to truncate and continue?')) {
                collect($tables)->each(function ($table) {
                    DB::connection(env('PGSQL_CONNECTION'))->table($table)->truncate();
                });
            } else {
                throw new Exception('User terminated migration.');
            }
        }
    }

    protected function migrateBrotherTree()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('brother_relations')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('brother_relations')
                    ->get()
                    ->map(function ($relation) {
                        return [
                            'user_id' => $relation->user_id,
                            'little_id' => $relation->little_id,
                        ];
                    })->toArray()
            );
        });
    }

    protected function migrateCorrespondence()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            $failures = collect([]);

            DB::connection(env('PGSQL_CONNECTION'))->table('correspondence')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('correspondence')
                    ->get()
                    ->map(function ($msg) use ($failures) {
                        collect(json_decode($msg->failures, true))->each(function ($failure) use ($failures) {
                            $user = DB::connection(env('PGSQL_CONNECTION'))
                                ->table('users')
                                ->where('email', $failure['recipient'])
                                ->first();

                            if ($user and array_key_exists('description', $failure)) {
                                $failures->push([
                                    'user_id' => $user->id,
                                    'error' => $failure['description'],
                                    'inserted_at' => (new Carbon)->setTimestamp($failure['timestamp']),
                                    'updated_at' => (new Carbon)->setTimestamp($failure['timestamp']),
                                ]);
                            }
                        });

                        return [
                            'id' => $msg->id,
                            'user_id' => $msg->user_id,
                            'subject' => $msg->subject,
                            'body' => $msg->body,
                            'opens' => $msg->opens,
                            'deliveries' => $msg->deliveries,
                            'inserted_at' => $msg->created_at,
                            'updated_at' => $msg->updated_at,
                        ];
                    })->toArray()
            );

            DB::connection(env('PGSQL_CONNECTION'))->table('email_failures')->insert($failures->toArray());
        });
    }

    protected function migrateEmailErrors()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('email_failures')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('users')
                    ->get()
                    ->filter(function ($user) {
                        return $user->email_failed != null;
                    })->map(function ($user) {
                        return [
                            'user_id' => $user->id,
                            'error' => $user->email_failed,
                            'inserted_at' => Carbon::now(),
                            'updated_at' => Carbon::now(),
                        ];
                    })->toArray()
            );
        });
    }

    protected function migratePayments()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('payments')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('payments')
                    ->get()
                    ->map(function ($payment) {
                        return [
                            'email' => $payment->email,
                            'name' => $payment->name,
                            'zip' => $payment->zip,
                            'product' => $payment->product,
                            'amount' => $payment->amount,
                            'cc_brand' => $payment->cc_brand,
                            'cc_lastfour' => $payment->cc_lastfour,
                            'token' => $payment->token,
                            'inserted_at' => $payment->created_at,
                            'updated_at' => $payment->updated_at,
                        ];
                    })->toArray()
            );
        });
    }

    protected function migrateUploads()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('files')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('files')
                    ->get()
                    ->map(function ($file) {
                        return [
                            'id' => $file->id,
                            'description' => $file->description,
                            'inserted_at' => $file->created_at,
                            'updated_at' => $file->updated_at,
                        ];
                    })->toArray()
            );

            DB::connection(env('PGSQL_CONNECTION'))->table('minutes')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('minutes')
                    ->get()
                    ->map(function ($file) {
                        return [
                            'id' => $file->id,
                            'meeting_date' => $file->meeting_date,
                            'inserted_at' => $file->created_at,
                            'updated_at' => $file->updated_at,
                        ];
                    })->toArray()
            );

            DB::connection(env('PGSQL_CONNECTION'))->table('newsletters')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('newsletters')
                    ->get()
                    ->map(function ($file) {
                        return [
                            'id' => $file->id,
                            'name' => $file->name,
                            'inserted_at' => $file->created_at,
                            'updated_at' => $file->updated_at,
                        ];
                    })->toArray()
            );

            DB::connection(env('PGSQL_CONNECTION'))->table('uploads')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('uploads')
                    ->get()
                    ->map(function ($file) {
                        $uploadableMap = [
                            'App\Models\File' => 'Coyote.Uploads.File',
                            'App\Models\Minutes' => 'Coyote.Uploads.Minute',
                            'App\Models\Newsletter' => 'Coyote.Uploads.Newsletter',
                        ];

                        return [
                            'id' => $file->id,
                            'file_name' => $file->file_name,
                            'file_path' => $file->file_path,
                            'size' => $file->size,
                            'mime' => $file->mime,
                            'token' => $file->token,
                            'uploadable_id' => $file->uploadable_id,
                            'uploadable_type' => $uploadableMap[$file->uploadable_type],
                            'inserted_at' => $file->created_at,
                            'updated_at' => $file->updated_at,
                        ];
                    })->toArray()
            );
        });
    }

    protected function migrateUsers()
    {
        DB::connection(env('PGSQL_CONNECTION'))->transaction(function () {
            DB::connection(env('PGSQL_CONNECTION'))->table('users')->insert(
                DB::connection(env('MYSQL_CONNECTION'))
                    ->table('users')
                    ->get()
                    ->map(function ($user) {
                        return [
                            'id' => $user->id,
                            'role_id' => $user->is_admin ? 3 : 2,
                            'email' => $user->email,
                            'password_hash' => $user->password,
                            'email_verified_at' => $user->active ? Carbon::now() : null,
                            'receives_email' => $user->subscribed,
                            'first_name' => $user->first_name,
                            'last_name' => $user->last_name,
                            'address' => $this->reformatAddress($user->address1, $user->address2, $user->city, $user->state, $user->zip),
                            'phone' => $user->phone,
                            'grad_year' => $user->grad_year,
                            'roll_number' => $user->roll_number,
                            'employer' => $user->employer,
                            'latitude' => $user->latitude,
                            'longitude' => $user->longitude,
                            'lifetime_member' => $user->lifetime_member > 0,
                            'inserted_at' => $user->created_at,
                            'updated_at' => $user->updated_at,
                        ];
                    })->toArray()
            );
        });
    }

    protected function reformatAddress($street1, $street2, $city, $state, $zip)
    {
        $base = $street1;

        if (strlen($street2) > 0)
            $base .= " $street2";

        if (strlen($city) > 0)
            $base .= ", $city";

        if (strlen($state) > 0)
            $base .= ", $state";

        if (strlen($zip) > 0)
            $base .= " $zip";

        return $base;
    }
}
