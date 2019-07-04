import dotenv from 'dotenv';
dotenv.config();

export default {
    sourcefile: process.env.SMSXMLFILE.trim()
}