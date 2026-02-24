

let resendClient = null;

async function getResendClient() {
  if (resendClient !== null) return resendClient;
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    if (process.env.NODE_ENV === "production") {
      console.warn("RESEND_API_KEY not configured. Email functionality disabled.");
    }
    return null;
  }
  try {
    const { Resend } = await import("resend");
    resendClient = new Resend(apiKey);
    return resendClient;
  } catch (error) {
    console.error("Failed to initialize Resend client:", error);
    return null;
  }
}


function getPasswordResetEmailHtml(resetLink) {
  const escapedLink = resetLink.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Reset your password</title>
</head>
<body style="margin:0; padding:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f1f5f9;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color: #f1f5f9; padding: 40px 20px;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width: 480px; margin: 0 auto;">
          <tr>
            <td style="background-color: #2563eb; border-radius: 16px 16px 0 0; padding: 32px 24px; text-align: center;">
              <h1 style="margin: 0; color: #ffffff; font-size: 24px; font-weight: 700; letter-spacing: -0.5px;">Incentive Calculator</h1>
              <p style="margin: 8px 0 0 0; color: #e0e7ff; font-size: 14px;">Password reset</p>
            </td>
          </tr>
          <tr>
            <td style="background-color: #ffffff; padding: 32px 24px; border-radius: 0 0 16px 16px; border: 1px solid #e2e8f0; border-top: none;">
              <p style="margin: 0 0 16px 0; color: #334155; font-size: 16px; line-height: 1.6;">You requested a password reset. Click the button below to set a new password. This link expires in <strong>1 hour</strong>.</p>
              <p style="margin: 0 0 24px 0; color: #64748b; font-size: 14px; line-height: 1.5;">If you didn't request this, you can safely ignore this email.</p>
              <table role="presentation" cellspacing="0" cellpadding="0" style="margin: 0 auto;">
                <tr>
                  <td style="border-radius: 10px; background-color: #2563eb;">
                    <a href="${escapedLink}" target="_blank" rel="noopener" style="display: inline-block; padding: 14px 28px; color: #ffffff; font-size: 16px; font-weight: 600; text-decoration: none;">Reset password</a>
                  </td>
                </tr>
              </table>
              <p style="margin: 24px 0 0 0; color: #94a3b8; font-size: 12px; line-height: 1.5;">If the button doesn't work, copy and paste this link into your browser:</p>
              <p style="margin: 8px 0 0 0; word-break: break-all;"><a href="${escapedLink}" style="color: #2563eb; text-decoration: none; font-size: 12px;">${escapedLink}</a></p>
            </td>
          </tr>
          <tr>
            <td style="padding: 24px 24px 0; text-align: center;">
              <p style="margin: 0; color: #94a3b8; font-size: 12px;">&copy; Incentive Calculator. This is an automated message.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`.trim();
}

/**
 * Send password reset email with attractive HTML template
 * @param {string} toEmail - Recipient email address
 * @param {string} resetLink - Password reset link
 * @returns {Promise<{sent: boolean, error?: string}>}
 */
export async function sendPasswordResetEmail(toEmail, resetLink) {
  const resend = await getResendClient();
  if (!resend) {
    return { sent: false, error: "Resend client not configured" };
  }
  
  const from = process.env.RESEND_FROM_EMAIL || "noreply@ritwil.com";
  
  try {
    const { data, error } = await resend.emails.send({
      from,
      to: toEmail,
      subject: "Reset your password – Incentive Calculator",
      text: `You requested a password reset. Use this link (valid for 1 hour): ${resetLink}\n\nIf you didn't request this, ignore this email.`,
      html: getPasswordResetEmailHtml(resetLink),
    });

    if (error) {
      console.error("Resend API error:", error);
      return { sent: false, error: error.message || "Failed to send email" };
    }

    if (process.env.NODE_ENV !== "production") {
      console.log("Email sent successfully:", data);
    }

    return { sent: true };
  } catch (error) {
    console.error("Failed to send email via Resend:", error);
    return { 
      sent: false, 
      error: error.message || "Unexpected error sending email" 
    };
  }
}

/**
 * Check if email is configured
 * @returns {boolean}
 */
export function isEmailConfigured() {
  return Boolean(process.env.RESEND_API_KEY);
}
